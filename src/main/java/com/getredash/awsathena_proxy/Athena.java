package com.getredash.awsathena_proxy;

import com.amazonaws.athena.jdbc.shaded.com.amazonaws.services.athena.model.InvalidRequestException;
import com.google.gson.annotations.SerializedName;

import java.sql.*;
import java.util.*;

class Column {
    String type;
    String name;
    @SerializedName("friendly_name")
    String friendlyName;

    Column(String type, String name, String friendlyName) {
        this.type = type;
        this.name = name;
        this.friendlyName = friendlyName;
    }
}

class Results {
    List<Column> columns;
    List<HashMap<String, Object>> rows;

    public Results(List<Column> columns, List<HashMap<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
    }
}

public class Athena {
    private Properties info;
    private String athenaUrl;

    private static final HashMap<Integer, String> typesMap = new HashMap<>();
    static {
        typesMap.put(Types.BIGINT, "integer");
        typesMap.put(Types.INTEGER, "integer");
        typesMap.put(Types.TINYINT, "integer");
        typesMap.put(Types.SMALLINT, "integer");
        typesMap.put(Types.FLOAT, "float");
        typesMap.put(Types.DOUBLE, "float");
        typesMap.put(Types.BOOLEAN, "boolean");
        typesMap.put(Types.VARCHAR, "string");
        typesMap.put(Types.NVARCHAR, "string");
        typesMap.put(Types.DATE, "date");
        typesMap.put(Types.TIME, "date");
        typesMap.put(Types.TIMESTAMP, "date");
    }

    public Athena(String athenaUrl, String awsAccessKey, String awsSecretKey, String s3StagingDir) {
        this.athenaUrl = athenaUrl;
        this.info = new Properties();
        this.info.put("user", awsAccessKey);
        this.info.put("password", awsSecretKey);
        this.info.put("s3_staging_dir", s3StagingDir);
    }

    public Results runQuery(String query) throws AthenaException {
        Connection conn = null;
        Statement statement = null;

        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");

            conn = DriverManager.getConnection(this.athenaUrl, this.info);

            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(query);
            ResultSetMetaData metadata = rs.getMetaData();

            List<Column> columns = new ArrayList<Column>();
            for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                columns.add(new Column(
                        typesMap.getOrDefault(metadata.getColumnType(i), "string"),
                        metadata.getColumnName(i),
                        metadata.getColumnLabel(i)));
            }

            List<HashMap<String, Object>> rows = new ArrayList<HashMap<String, Object>>();

            while (rs.next()) {
                HashMap<String, Object> row = new HashMap<>();

                for (int i = 1; i <= metadata.getColumnCount(); ++i) {
                    row.put(metadata.getColumnName(i), rs.getObject(i));
                }

                rows.add(row);
            }

            rs.close();
            conn.close();

            return new Results(columns, rows);
        } catch (InvalidRequestException ex) {
            throw new AthenaException(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            throw new AthenaException("Error initializing AWS Athena proxy.");
        } catch (SQLException ex) {
            if (ex.getCause() instanceof InvalidRequestException) {
                throw new AthenaException(ex.getCause().getMessage());
            }
            throw new AthenaException(ex.getMessage());
        }
    }
}
