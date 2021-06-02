package ua.co.k.snowflake.sample;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyMap;

@SuppressWarnings("SpringJavaAutowiredFieldsWarningInspection")
@SpringBootApplication
@Slf4j
public class Runner {
    private Path fileToLoad = Paths.get("src", "main", "resources", "sample.csv");
    
    public static void main(String[] args) {
        SpringApplication.run(Runner.class, args);
    }
    
    @Configuration
    public static class Conf {
        @Bean
        public SnowPipeIntegrationHelper snowPipeIntegrationHelper() throws Exception {
            return new SnowPipeIntegrationHelper(
                    "pl18490.europe-west4.gcp.snowflakecomputing.com",
                    "msangel",
                    "DEMO_DB.PUBLIC.NAMES_PIPE");
        }
    }

    @Autowired
    private NamedParameterJdbcTemplate template;
    
    @Autowired
    private SnowPipeIntegrationHelper helper;

    @EventListener(ApplicationReadyEvent.class)
    public void ready() throws Exception {
        helper.logHistory(null, null);
//        uploadAndMoveStrategy();
        processFromSnowPipe();
    }

    private void processFromSnowPipe() throws Exception {
        String pipes = template.execute("show pipes;", this::statementToString);
        log.info("pipes: {}", pipes);
        // works only if uncomment these lines, very weird
        // possible answer: https://snowflakecommunity.force.com/s/question/0D50Z00009bSpIdSAK/how-can-i-load-a-csv-with-the-same-name-as-a-csv-that-had-already-been-loaded-with-snowpipe-
//      https://stackoverflow.com/questions/59184903/snowpipe-not-working-after-upload-same-file-twice
        // also take a look: https://community.snowflake.com/s/article/HowTo-Configuration-steps-for-Snowpipe-Auto-Ingest-with-AWS-S3-Stages
//      http://bifuture.blogspot.com/2020/11/snowflake-loading-data-with-snowpipe-on.html
//        template.execute("create or replace pipe DEMO_DB.PUBLIC.NAMES_PIPE as\n" +
//                "copy into DEMO_DB.PUBLIC.NAMES from @DEMO_DB.PUBLIC.INTERNAL_STAGE\n" +
//                "    file_format = (format_name='MY_FORMAT');", PreparedStatement::execute);
        pipes = template.execute("show pipes;", this::statementToString);
        log.info("pipes: {}", pipes);
        
        template.update("delete from NAMES;", emptyMap());
        Integer countBefore = template.queryForObject("select count(*) from NAMES", emptyMap(), Integer.class);
        if(!Files.exists(fileToLoad)) {
            throw new RuntimeException("file not exists: " + fileToLoad.toAbsolutePath());
        }
        String fileLocation = "file://" + fileToLoad.toAbsolutePath();
        template.execute("PUT " + fileLocation + " @INTERNAL_STAGE OVERWRITE=TRUE", PreparedStatement::executeQuery);
        String simpleFileName = fileToLoad.getFileName().toString();

        log.info("countBefore: {}", countBefore);
        
        helper.consumeFile(simpleFileName);
        
        Integer countAfter = template.queryForObject("select count(*) from NAMES", emptyMap(), Integer.class);
        log.info("countAfter: {}", countAfter);
    }

    private String statementToString(PreparedStatement preparedStatement) throws SQLException {
        ResultSet rs = preparedStatement.executeQuery();
        List rows = new ArrayList();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            int columnCount = metaData.getColumnCount();
            String[] row = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = rs.getString(i);
            }
            rows.add(Arrays.toString(row));
        }
        return rows.toString();
    }

    private void uploadAndMoveStrategy() {
        template.execute("create or replace stage INTERNAL_STAGE;", PreparedStatement::execute);
        template.execute("create or replace table NAMES( id varchar(20), name varchar(20));", PreparedStatement::execute);
        template.execute("create or replace FILE FORMAT MY_FORMAT TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = NONE NULL_IF = ('\\\\N', '');", PreparedStatement::execute);
        template.update("delete from NAMES;", emptyMap());
        Integer countBefore = template.queryForObject("select count(*) from NAMES", emptyMap(), Integer.class);
        log.info("countBefore: {}", countBefore);
        String fileLocation = "file://" + fileToLoad.toAbsolutePath();
        template.execute("PUT " + fileLocation + " @INTERNAL_STAGE overwrite=true", PreparedStatement::execute);
        String simpleFileName = fileToLoad.getFileName().toString();
        template.execute("copy into NAMES from @INTERNAL_STAGE/" + simpleFileName + " "
                + "file_format = (format_name = 'MY_FORMAT') purge = true;", PreparedStatement::execute);
        Integer countAfter = template.queryForObject("select count(*) from NAMES", emptyMap(), Integer.class);
        log.info("countAfter: {}", countAfter);
    }

}

// some reading:
// https://toppertips-bx67a.ondigitalocean.app/snowpro-snowpipe-cheat-sheet/