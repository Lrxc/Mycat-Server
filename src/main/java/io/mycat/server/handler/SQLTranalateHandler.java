package io.mycat.server.handler;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.model.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLTranalateHandler {
    private static final Logger logger = LoggerFactory.getLogger(SQLTranalateHandler.class);

    /**
     * 特殊字符需修
     *
     * @param sql    sql
     * @param dbType 数据库类型
     */
    public static String trim(String sql, String dbType) {
        String transSql = sql;

        switch (dbType.toLowerCase()) {
            case "mysql":
            case "postgre":
                transSql = sql;
                break;
            case "oracle":
                transSql = sql.replace("`", "\"");
                break;
        }

        logger.debug("sql: " + sql);
        logger.debug("transSql-[{}]: {} \n", dbType, transSql);
        return transSql;
    }

    /**
     * mysql语法翻译成其他sql语言
     *
     * @param sql  原sql
     * @param user 执行该sql的用户名
     */
    public static String tranlate(String sql, String user) {
        String db = MycatServer.getInstance().getConfig().getUsers().get(user).getDefaultSchema();
        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
        PhysicalDBNode physicalDBNode = MycatServer.getInstance().getConfig().getDataNodes().get(schema.getDataNode());

        String transSql = sql;

        String dbType = schema.getDefaultDataNodeDbType().toLowerCase();
        switch (dbType) {
            case "mysql":
            case "postgre":
                transSql = sql;
                break;
            case "oracle":
                transSql = oracle(sql, physicalDBNode.getDatabase());
                break;
        }

        //try {
        //    List<SQLStatement> sqlStatements = SQLUtils.parseStatements(transSql, DbType.oracle);
        //    transSql = SQLUtils.toSQLString(sqlStatements, DbType.oracle);
        //} catch (Exception e) {
        //}
        return transSql;
    }

    private static String oracle(String sql, String schemaName) {
        try {
            if (sql.equalsIgnoreCase("select 1"))
                sql = "select 1 from dual";
            if ("SHOW TABLES".equalsIgnoreCase(sql)) {
                sql = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER IN (" + schemaName + ")";
            } else if (sql.contains("SHOW FULL TABLES")) {
                sql = "SELECT TABLE_NAME AS OBJECT_NAME,'BASE TABLE' AS Table_type FROM ALL_TABLES WHERE OWNER IN ('" + schemaName + "')";
            } else if ("SHOW FULL TABLES WHERE Table_type != 'VIEW'".equalsIgnoreCase(sql)) {
                sql = " SELECT OBJECT_NAME,'BASE TABLE' AS Table_type FROM USER_OBJECTS WHERE OBJECT_TYPE='TABLE' ";
            } else if ("SHOW TABLE STATUS".equalsIgnoreCase(sql)) {
                sql = "SELECT TABLE_NAME AS OBJECT_NAME,'BASE TABLE' AS Table_type FROM ALL_TABLES WHERE OWNER IN ('" + schemaName + "')";
            } else if (sql.contains("SHOW CREATE TABLE")) {
                String tableName = sql.split("SHOW CREATE TABLE ")[1].replaceAll("`", "'");
                sql = "select " + tableName + " AS \"Table\",xmlagg(xmlparse(content dbms_metadata.get_ddl(u.object_type, u.object_name," + schemaName + ") ||'**********' wellformed) order by u.object_type desc).getclobval() AS \"Create Table\"  from dba_objects u \nwhere u.object_type in ('TABLE','INDEX','PROCEDURE','FUNCTION') and ((u.object_name = " + tableName + " AND u.owner = " + schemaName + " ) or u.object_name in (select INDEX_NAME from dba_indexes where table_name=" + tableName + " AND table_owner = " + schemaName + "));";
            } else if (sql.contains("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '")) {
                sql = "SELECT t1.OWNER AS \"TABLE_SCHEMA\",t1.TABLE_NAME,t2.COLUMN_NAME, \ncase when t2.column_length is null then  t2.DATA_TYPE \nELSE  CONCAT(CONCAT(t2.DATA_TYPE,'('),CONCAT(t2.column_length,')'))\nEND AS \"COLUMN_TYPE\"\nFROM\n(SELECT a.OWNER,a.TABLE_NAME,d.COMMENTS FROM all_tables a LEFT JOIN all_tab_comments d ON a.OWNER = d.OWNER AND a.TABLE_NAME = d.TABLE_NAME WHERE a.OWNER = " + schemaName + " ) t1,\n(SELECT b.OWNER,b.TABLE_NAME,b.COLUMN_NAME,c.COMMENTS,b.DATA_TYPE,b.COLUMN_ID,decode( data_type, 'NUMBER', decode( data_precision, NULL, '', data_precision || ',' || data_scale ), char_col_decl_length ) AS column_length \nFROM all_tab_cols b LEFT JOIN all_col_comments c ON b.OWNER = c.OWNER AND b.TABLE_NAME = c.TABLE_NAME AND b.COLUMN_NAME = c.COLUMN_NAME \nWHERE b.OWNER = " + schemaName + " AND c.OWNER = " + schemaName + " ORDER BY b.TABLE_NAME,b.COLUMN_ID ) t2 \nWHERE t1.OWNER = " + schemaName + " AND t2.OWNER = " + schemaName + " AND t1.OWNER = t2.OWNER AND t1.TABLE_NAME = t2.TABLE_NAME ORDER BY t1.TABLE_NAME,t2.COLUMN_ID";
            } else if (sql.contains("SHOW FULL COLUMNS FROM")) {
                String tableName = sql.split("SHOW FULL COLUMNS FROM ")[1].replaceAll("`", "'");
                sql = "SELECT t2.COLUMN_NAME AS \"Field\", case when t2.column_length is null then  t2.DATA_TYPE ELSE  CONCAT(CONCAT(t2.DATA_TYPE,'('),CONCAT(t2.column_length,')')) END AS \"Type\",'utf8mb4_general_ci' AS \"Collatile\",'YES' AS \"Null\",' ' AS \"Key\",t2.DATA_DEFAULT AS \"Default\",' ' AS \"Extra\",'select,insert,update,references' AS \"Privileges\",t2.COMMENTS AS \"Comment\" \nFROM (SELECT a.OWNER,a.TABLE_NAME,d.COMMENTS FROM all_tables a LEFT JOIN all_tab_comments d ON a.OWNER = d.OWNER AND a.TABLE_NAME = d.TABLE_NAME WHERE a.OWNER = " + schemaName + " AND a.TABLE_NAME =" + tableName + " ) t1,\n(SELECT b.OWNER,b.TABLE_NAME,b.COLUMN_NAME,c.COMMENTS,b.DATA_TYPE,b.COLUMN_ID,b.DATA_DEFAULT,decode( data_type, 'NUMBER', decode( data_precision, NULL, '', data_precision || ',' || data_scale ),char_col_decl_length ) AS column_length FROM all_tab_cols b LEFT JOIN all_col_comments c ON b.OWNER = c.OWNER AND b.TABLE_NAME = c.TABLE_NAME AND b.COLUMN_NAME = c.COLUMN_NAME WHERE b.OWNER = " + schemaName + " AND c.OWNER = " + schemaName + " ORDER BY b.TABLE_NAME,b.COLUMN_ID ) t2 WHERE t1.OWNER = " + schemaName + " AND t2.OWNER = " + schemaName + " AND t1.OWNER = t2.OWNER AND t1.TABLE_NAME = t2.TABLE_NAME ORDER BY t1.TABLE_NAME,t2.COLUMN_ID";
            } else if ("SELECT @@character_set_database, @@collation_database".equalsIgnoreCase(sql)) {
                sql = "SELECT * FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER= 'NLS_CHARACTERSET' ";
            } else if (sql.trim().length() >= 4 && sql.trim().substring(0, 4).toUpperCase().contains("DESC")) {
                sql = "SELECT A.column_name AS \"Name\", A.DATA_TYPE || '(' || A.DATA_LENGTH || ')' AS \"Type\", A.nullable AS \"Nullable\", A.data_default AS \"Defaule\", B.comments AS \"Comments\" FROM USER_TAB_COLUMNS A JOIN user_col_comments B ON A.table_name = b.table_name WHERE A.table_name = '" + sql.trim().substring(4, sql.length()).trim() + "' AND A.column_name = B.column_name;";
            } else if (sql.toUpperCase().contains("LIMIT") && sql.contains("`.`")) {
                String[] arrayOfString1 = sql.split("LIMIT|limit");
                String[] sqlSplitNavicat = sql.split("`");
                if (arrayOfString1.length > 0 && (sqlSplitNavicat.length != 5 || !sql.contains("SELECT * FROM `" + schemaName + "`.`" + sqlSplitNavicat[3]))) {
                    String[] page = arrayOfString1[1].split(",");
                    if (page.length > 1) {
                        //sql = "select * from(select a.*,ROWNUM rn from(" + arrayOfString1[0] + ") a where ROWNUM<=(" + page[1] + ")) where rn>" + page[0];
                        sql = "select * from(select a.* from(" + arrayOfString1[0] + ") a where ROWNUM<=(" + page[1] + ")) where ROWNUM>" + page[0];
                    } else {
                        sql = "select * from(select a.*,ROWNUM rn from(" + arrayOfString1[0] + ") a where ROWNUM<=(" + page[0] + ")) where rn>" + Character.MIN_VALUE;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Oracle语法转换失败", e);
        }
        return sql;
    }
}
