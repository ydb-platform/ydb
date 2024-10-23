# Default ports assigned to {{ ydb-short-name }}

In the instructions for working with {{ ydb-short-name }}, the following main ports assigned by default are used:

| Port  | Purpose                                             |
|-------|-----------------------------------------------------|
| 2135  | Port for secure gRPC connections (TLS).             |
| 2136  | Port for gRPC connections.                          |
| 19001 | Interconnect port.                                  |
| 8765  | HTTPS port for cluster monitoring.                  |
| 4317  | Port for query tracing logs.                        |

Secondary ports configured by default for integrations and external tools:

| Port  | Purpose                                                                 |
|-------|-------------------------------------------------------------------------|
| 2130  | Port for connecting the connector.                                      |
| 5432  | Port for connecting to PostgreSQL.                                      |
| 3306  | Port for connecting to MySQL.                                           |
| 1521  | Port for connecting to Oracle databases.                                |
| 5000  | Port for connecting to DB2 databases.                                   |
| 9088  | Port for connecting to the Informix server.                             |
| 9876  | Port for listening for incoming data.                                   |
| 9093  | Port for listening to encrypted SSL/TLS connections for Apache Kafka.   |

Ports can be reassigned at your discretion.