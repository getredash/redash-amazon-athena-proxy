package com.getredash.awsathena_proxy;

import com.google.gson.Gson;
import spark.Request;
import spark.Response;

import static spark.Spark.*;

class QueryRequest {
    String athenaUrl;
    String awsAccessKey;
    String awsSecretKey;
    String s3StagingDir;
    String query;
}

public class API {
    public static void main(String[] args) {
        System.setProperty("java.util.logging.SimpleFormatter.format",  "%1$tF %1$tT %4$s %2$s %5$s%6$s%n");

        String port = System.getenv("PORT");
        if (port != null) {
            port(Integer.valueOf(port));
        }

        Gson gson = new Gson();
        post("/query", API::queryRequest, gson::toJson);
        get("/ping", (req, res) -> "PONG");
    }

    public static Object queryRequest(Request req, Response res) {
        Gson gson = new Gson();
        QueryRequest body = gson.fromJson(req.body(), QueryRequest.class);
        Athena athena = new Athena(body.athenaUrl, body.awsAccessKey, body.awsSecretKey, body.s3StagingDir);

        try {
            Results results = athena.runQuery(body.query);
            return results;
        } catch (AthenaException e) {
            halt(400, e.getMessage());
        }
        return null;
    }
}
