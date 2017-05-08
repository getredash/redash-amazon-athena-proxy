# Redash Amazon Athena Proxy

Simple proxy to provide REST API to Amazon Athena without the need to use the JDBC driver directly.
Returns the results in a format compatible with Redash.

## Usage

Docker:

```
docker run redash/amazon-athena-proxy
```

By default the proxy will listen on port 4567, but this can be changed by specifying a different port using the PORT environment variable.

