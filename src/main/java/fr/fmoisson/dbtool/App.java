package fr.fmoisson.dbtool;

import fr.fmoisson.dbtool.config.AppConfig;
import fr.fmoisson.dbtool.sql.SqlGenerator;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

/**
 * Hello world!
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    public static final String OPTION_DDL = "ddl";
    public static final String OPTION_SCHEMA = "schema";
    public static final String OPTION_SYNCDML = "syncdml";

    public static void main(String[] args) {
        boolean printHelp = true;

        // cmdline arguments definition
        Options options = new Options();
        options.addOption(OPTION_SYNCDML, false, "Generate Sync DML SQL");
        options.addOption(OPTION_DDL, false, "Generate DDL SQL");
        options.addOption(OPTION_SCHEMA, true, "Database schema");

        try {
            CommandLineParser parser = new BasicParser();
            CommandLine cmd = parser.parse(options, args);

            List<String> argList = (List<String>) cmd.getArgList();
            if (argList != null && !argList.isEmpty() && cmd.hasOption(OPTION_SCHEMA)) {
                String schema = cmd.getOptionValue(OPTION_SCHEMA);

                // start spring context
                ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);

                // test connection
                DataSource ds = (DataSource) context.getBean("dataSource");
                try {
                    ds.getConnection();
                    logger.info("SQL Connection OK.");
                } catch (SQLException e) {
                    logger.error("SQL Connection KO.", e);
                    return;
                }

                // getting sql generator
                // TODO : implementation switch according to db type
                SqlGenerator ddlGenerator = context.getBean(SqlGenerator.class);

                // start operation according to options
                if (cmd.hasOption(OPTION_DDL)) {
                    printHelp = false;
                    argList.forEach((tableName) -> {
                        logger.info("Generating DDL for " + tableName);
                        String instruction = ddlGenerator.generateDDL(schema, tableName);

                        try {
                            // writing sql in separate file
                            Files.write(Paths.get("DDL_" + tableName + ".sql"), instruction.getBytes());
                        } catch (IOException e) {
                            logger.error("IO Exception", e);
                        }
                    });
                    logger.info("DDL generations terminated.");
                }

                if (cmd.hasOption(OPTION_SYNCDML)) {
                    printHelp = false;
                    argList.forEach((tableName) -> {
                        logger.info("Generating Sync DML for " + tableName);
                        List<String> listInstructions = ddlGenerator.generateSyncDML(schema, tableName);

                        try {
                            // writing sql in separate file
                            Files.write(Paths.get("SYNC_DML_" + tableName + ".sql"), listInstructions);
                        } catch (IOException e) {
                            logger.error("IO Exception", e);
                        }
                    });
                    logger.info("Sync DML generations terminated.");
                }
            }
        } catch (ParseException e) {
            logger.error("Unrecognized or malformed cmdline options.", e);
        }

        if (printHelp) {
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        // print help if
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java -jar db-tool.jar -schema <schema> [OPTIONS] [TABLE_1] [TABLE_2] ...", "Export SQL dump to file.", options, "");
    }


}
