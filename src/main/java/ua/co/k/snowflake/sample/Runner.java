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
        // works only if uncomment these lines, very weird
//        template.execute("create or replace pipe DEMO_DB.PUBLIC.NAMES_PIPE as\n" +
//                "copy into DEMO_DB.PUBLIC.NAMES from @DEMO_DB.PUBLIC.INTERNAL_STAGE\n" +
//                "    file_format = (format_name='MY_FORMAT');", PreparedStatement::execute);
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
