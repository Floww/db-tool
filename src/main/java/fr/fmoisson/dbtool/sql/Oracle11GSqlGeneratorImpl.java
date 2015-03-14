package fr.fmoisson.dbtool.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by fmoisson on 12/03/15.
 */
@Service
public class Oracle11GSqlGeneratorImpl implements SqlGenerator {

    private static final Logger logger = LoggerFactory.getLogger(Oracle11GSqlGeneratorImpl.class);

    private static final String SET_SEPARATOR = ", ";
    private static final String WHERE_SEPARATOR = " AND ";
    private static final String DATETIME_JAVA_PATTERN = "yyyyMMddHHmmss";
    private static final String DATETIME_ORACLE_PATTERN = "yyyyMMddHH24miss";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat(DATETIME_JAVA_PATTERN);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public String generateDDL(String schema, String tableName) {
        // prevent to have some useless sql for us
        jdbcTemplate.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE','FALSE'); END;");
        jdbcTemplate.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE','FALSE'); END;");
        jdbcTemplate.execute("BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES','FALSE'); END;");

        return jdbcTemplate.queryForObject("SELECT dbms_metadata.get_ddl('TABLE', ?, ?) FROM dual", new Object[]{tableName, schema}, String.class);
    }

    @Override
    public List<String> generateSyncDML(String schema, String tableName) {
        List<String> listColumnPK = jdbcTemplate.queryForList("SELECT column_name " +
                        "FROM all_cons_columns " +
                        "WHERE constraint_name = (" +
                        "  SELECT constraint_name " +
                        "  FROM user_constraints " +
                        "  WHERE UPPER(table_name) = UPPER(?) " +
                        "  AND CONSTRAINT_TYPE = 'P')",
                new Object[]{tableName}, String.class);
        logger.info("Columns of primay key : " + listColumnPK.toString());

        List<String> listColumnNonNullable = jdbcTemplate.queryForList("SELECT column_name FROM all_tab_columns " +
                        "WHERE UPPER(table_name) = UPPER(?) " +
                        "AND NULLABLE = 'N' " +
                        "AND column_name NOT IN (" +
                        "  SELECT column_name " +
                        "  FROM all_cons_columns " +
                        "  WHERE constraint_name = (" +
                        "    SELECT constraint_name " +
                        "    FROM user_constraints " +
                        "    WHERE UPPER(table_name) = UPPER(?) " +
                        "    AND CONSTRAINT_TYPE = 'P'" +
                        "  )" +
                        ")",
                new Object[]{tableName, tableName}, String.class);
        logger.info("Columns non nullable : " + listColumnNonNullable.toString());

        List<String> listColumnNullable = jdbcTemplate.queryForList("SELECT column_name FROM all_tab_columns " +
                        "WHERE UPPER(table_name) = UPPER(?) " +
                        "AND NULLABLE = 'Y' " +
                        "AND column_name NOT IN (" +
                        "  SELECT column_name " +
                        "  FROM all_cons_columns " +
                        "  WHERE constraint_name = (" +
                        "    SELECT constraint_name " +
                        "    FROM user_constraints " +
                        "    WHERE UPPER(table_name) = UPPER(?) " +
                        "    AND CONSTRAINT_TYPE = 'P'" +
                        "  )" +
                        ")",
                new Object[]{tableName, tableName}, String.class);
        logger.info("Columns nullable : " + listColumnNullable.toString());

        List<Map<String, Object>> results = jdbcTemplate.queryForList("SELECT * FROM " + schema + '.' + tableName);

        // sql instructions
        List<String> sql = new ArrayList();

        // generate insert instructions
        sql.add("-- Inserts new records of " + tableName);
        results.forEach(row -> {
            final StringBuilder instructions = new StringBuilder("INSERT INTO ");
            instructions.append(tableName);
            instructions.append('(');

            // append columns pk and non nullable
            final AtomicInteger i = new AtomicInteger(0);
            listColumnPK.forEach(col -> appendColumn(instructions, col, i.getAndIncrement()));

            i.set(0);
            listColumnNonNullable.forEach(col -> appendColumn(instructions, col, i.getAndIncrement()));

            instructions.append(") SELECT ");

            // append column value of pk and non nullable
            i.set(0);
            listColumnPK.forEach(col -> appendColumnValue(instructions, row.get(col), i.getAndIncrement()));

            i.set(0);
            listColumnNonNullable.forEach(col -> appendColumnValue(instructions, row.get(col), i.getAndIncrement()));

            instructions.append(" FROM dual WHERE NOT EXISTS (SELECT NULL FROM ");
            instructions.append(tableName);
            instructions.append(" WHERE ");

            // append filter
            i.set(0);
            listColumnPK.forEach(col -> appendColumnFilter(instructions, col, row.get(col), i.getAndIncrement(), WHERE_SEPARATOR));

            instructions.append(");");

            sql.add(instructions.toString());
            // -- Completion of key and non nullable
            // INSERT INTO <table> (<cle1>, <cle2>, <non_null1>, <non_null2>)
            // SELECT <cle1_val>, <cle2_val>, <non_null1_vol>, <non_null2_vol> FROM dual
            // WHERE NOT EXISTS (SELECT NULL FROM <table> WHERE <cle1> = <cle1_val> AND <cle2> = <cle2_val>);
        });

        // Generate update instructions
        sql.add("-- Updates records of " + tableName);
        results.forEach(row -> {
            final StringBuilder instructions = new StringBuilder("UPDATE ");
            instructions.append(tableName);
            instructions.append(" SET ");

            // append setter nullable column
            final AtomicInteger i = new AtomicInteger(0);
            listColumnNullable.forEach(col -> appendColumnFilter(instructions, col, row.get(col), i.getAndIncrement(), SET_SEPARATOR));

            instructions.append(" WHERE ");

            // append filter
            i.set(0);
            listColumnPK.forEach(col -> appendColumnFilter(instructions, col, row.get(col), i.getAndIncrement(), WHERE_SEPARATOR));

            instructions.append(";");

            sql.add(instructions.toString());
            // -- Update data except key
            // UPDATE <table> SET <null1> = '<null1_val>' WHERE <cle1> = <cle1_val>;
        });

        // Generate delete instruction
        sql.add("-- Deletes other records of " + tableName);
        final StringBuilder instructions = new StringBuilder("DELETE FROM ");
        instructions.append(tableName);
        instructions.append(" WHERE (");

        // Append PK
        final AtomicInteger i = new AtomicInteger(0);
        listColumnPK.forEach(col -> appendColumn(instructions, col, i.getAndIncrement()));

        instructions.append(") NOT IN (");

        // Append pk value to keep
        i.set(0);
        results.forEach(row -> {
            if (i.getAndIncrement() > 0) {
                instructions.append(",");
            }

            instructions.append("(");

            // append setter nullable column
            final AtomicInteger j = new AtomicInteger(0);
            listColumnPK.forEach(col -> appendColumnValue(instructions, row.get(col), j.getAndIncrement()));

            instructions.append(")");
        });

        instructions.append(");");
        sql.add(instructions.toString());

        return sql;
    }

    /**
     * Appends a column filter with a separator if i > 0
     *
     * @param instructions SQL instruction
     * @param column       Column name
     * @param value        Column value
     * @param i            Iteration
     * @param sep          Separator
     */
    private static void appendColumnFilter(StringBuilder instructions, String column, Object value, int i, String sep) {
        if (i > 0) {
            instructions.append(sep);
        }

        instructions.append(column);
        instructions.append(" = ");

        appendValue(instructions, value);
    }

    /**
     * Appends an SQL value
     *
     * @param instructions SQL instruction
     * @param value        Column value
     */
    private static void appendValue(StringBuilder instructions, Object value) {
        if (value == null) {
            instructions.append(value);
        } else if (value instanceof String) {
            instructions.append(StringUtils.quote((String) ((String) value).replaceAll("'", "''")));
        } else if (value instanceof Date) {
            instructions.append("to_date('");
            instructions.append(DATE_FORMAT.format(value));
            instructions.append("', '");
            instructions.append(DATETIME_ORACLE_PATTERN);
            instructions.append("')");
        } else {
            instructions.append(value.toString());
        }
    }

    /**
     * Appends an SQL value with a separator if i > 0.
     *
     * @param instructions SQL instruction
     * @param value        Column value
     * @param i            Iteration
     */
    private static void appendColumnValue(StringBuilder instructions, Object value, int i) {
        if (i > 0) {
            instructions.append(',');
        }

        appendValue(instructions, value);
    }

    /**
     * Appends an SQL column with a separator if i > 0
     *
     * @param instructions SQL instruction
     * @param column       Column name
     * @param i            Iteration
     */
    private static void appendColumn(StringBuilder instructions, String column, int i) {
        if (i > 0) {
            instructions.append(',');
        }
        instructions.append(column);
    }

}
