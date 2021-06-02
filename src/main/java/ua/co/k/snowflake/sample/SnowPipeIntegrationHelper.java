package ua.co.k.snowflake.sample;

import lombok.extern.slf4j.Slf4j;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponseException;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SnowPipeIntegrationHelper {

    private final String host;
    private final String username;
    private final String fullPipeName;
    private final SimpleIngestManager manager;

    public SnowPipeIntegrationHelper(String host, String username, String fullPipeName) throws Exception {
        this.host = host;
        this.username = username;
        this.fullPipeName = fullPipeName;
        final PrivateKey privateKey = PrivateKeyReader.get(System.getProperty("user.home") +"/.snowflake_keys/rsa_key.p8");
        this.manager = new SimpleIngestManager(host.split("\\.")[0], username, fullPipeName, privateKey, "https", host, 443);
    }

    public static class PrivateKeyReader
    {
        // If you generated an encrypted private key, implement this method to return
        // the passphrase for decrypting your private key.
        private static String getPrivateKeyPassphrase() {
//            return "<private_key_passphrase>";
            throw new RuntimeException("given key should no be protected using passphrase");
        }

        public static PrivateKey get(String filename)
                throws Exception
        {
            PrivateKeyInfo privateKeyInfo = null;
            Security.addProvider(new BouncyCastleProvider());
            // Read an object from the private key file.
            PEMParser pemParser = new PEMParser(new FileReader(Paths.get(filename).toFile()));
            Object pemObject = pemParser.readObject();
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Handle the case where the private key is encrypted.
                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                String passphrase = getPrivateKeyPassphrase();
                InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            } else if (pemObject instanceof PrivateKeyInfo) {
                // Handle the case where the private key is unencrypted.
                privateKeyInfo = (PrivateKeyInfo) pemObject;
            }
            pemParser.close();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            return converter.getPrivateKey(privateKeyInfo);
        }
    }
    
    @Slf4j
    static class GetHistory implements Callable<HistoryResponse> {
        private final Set<String> filesWatchList;
        private final SimpleIngestManager manager;

        GetHistory(Set<String> files, SimpleIngestManager manager) {
            this.filesWatchList = files;
            this.manager = manager;
        }
        String beginMark = null;

        public HistoryResponse call() throws Exception {
            HistoryResponse filesHistory = null;
            while (true) {
                Thread.sleep(500);
                HistoryResponse response = manager.getHistory(null, null, beginMark);
                if (response.getNextBeginMark() != null) {
                    beginMark = response.getNextBeginMark();
                }
                //noinspection ConstantConditions
                if (response == null) {
                    continue;
                }
                if (response.files != null) {
                    for (HistoryResponse.FileEntry entry : response.files) {
                        if (entry != null) {
                            log.info("entry path: {}, isComplete: {}", entry.getPath(), entry.isComplete());
                        } else {
                            log.info("file entry is null");
                        }
                        //if we have a complete file that we've
                        // loaded with the same name..
                        String filename = entry.getPath();
                        if (entry.getPath() != null && entry.isComplete() && filesWatchList.contains(filename)) {
                            if (filesHistory == null) {
                                filesHistory = new HistoryResponse();
                                filesHistory.setPipe(response.getPipe());
                            }
                            filesHistory.files.add(entry);
                            filesWatchList.remove(filename);
                            //we can return true!
                            if (filesWatchList.isEmpty()) {
                                return filesHistory;
                            }
                        }
                    }
                }
            }
        }
    }

    private HistoryResponse waitForFilesHistory(Set<String> files) throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();
        GetHistory historyCaller = new GetHistory(files, manager);
        //fork off waiting for a load to the service
        Future<HistoryResponse> result = service.submit(historyCaller);
        return result.get(2, TimeUnit.MINUTES);
    }

    public void consumeFile(String simpleFileName) throws Exception {
        final long oneHourMillis = 1000 * 3600L;
        String startTime = Instant
                .ofEpochMilli(System.currentTimeMillis() - (4*oneHourMillis)).toString();
        Set<String> files = new TreeSet<>();
        files.add(simpleFileName);
        manager.ingestFiles(SimpleIngestManager.wrapFilepaths(files), null);
        waitForFilesHistory(files);

        String endTime = Instant
                .ofEpochMilli(System.currentTimeMillis()).toString();
        logHistory(startTime, endTime);
    }
    
    public void logHistory(String startTime, String endTime) throws IngestResponseException, URISyntaxException, IOException {
        final long oneHourMillis = 1000 * 3600L;
        if (startTime == null) {
            startTime = Instant
                    .ofEpochMilli(System.currentTimeMillis() - 4 * oneHourMillis).toString();
        }
        if (endTime == null) {
            endTime = Instant
                    .ofEpochMilli(System.currentTimeMillis()).toString();
        }
        HistoryRangeResponse historyRangeResponse =
                manager.getHistoryRange(null,
                        startTime,
                        endTime);
        log.info("Received history range response: " +
                historyRangeResponse.toString());
    }
}
