package fr.fmoisson.dbtool.sql;

import java.util.List;

/**
 * Created by fmoisson on 12/03/15.
 */
public interface SqlGenerator {
    /**
     * Generate a DDL SQL instruction for table creation.
     * @param schema Schema
     * @param tableName Table name
     * @return Sql instruction
     */
    String generateDDL(String schema, String tableName) ;

    /**
     * Generate DML SQL instructions for table synchronization.
     * @param schema Schema
     * @param tableName Table name
     * @return List of Sql instructions
     */
    List<String> generateSyncDML(String schema, String tableName);
}
