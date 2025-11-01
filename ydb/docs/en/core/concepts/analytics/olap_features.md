# Key Analytics Capabilities: Quick Reference

This page is a map of the documentation for the analytical features of {{ydb-short-name}}. The content is organized by the stages of the data lifecycle to help you quickly find the information you need for designing, developing, and operating analytical solutions.

## Data Warehouse Design (Concepts & Design)

Core concepts for organizing, scaling, and managing data.

### Main Concepts and Data Types

  - [Column-Oriented Tables](../datamodel/table.md#column-oriented-tables): Storage architecture optimized for OLAP workloads.
  - [Data Types](../../yql/reference/types/index.md): Comprehensive reference for supported types.

### Scalability and Performance

  - [Primary Key Design for Maximum Performance](../../dev/primary-key/column-oriented.md): How to choose `PRIMARY KEY` and `PARTITION BY`.
  - [Table Partitioning](../datamodel/table.md#olap-tables-partitioning): Mechanism for distributing data across nodes.

### Data Lifecycle Management

  - [TTL (Time-to-Live)](../ttl.md): Automatic deletion of outdated data after expiration.

### Streaming Ingestion

  - [Topics (Kafka API)](../datamodel/topic.md): Native streaming using the Kafka protocol.
  - [Connector Fluent Bit](../../integrations/ingestion/fluent-bit.md): Direct log ingestion.

### Batch Ingestion

  - [Apache Spark Connector](../../integrations/ingestion/spark.md): Read and write data for ETL/ELT tasks.
  - [BulkUpsert API](../../recipes/ydb-sdk/bulk-upsert.md): High-performance bulk inserts via SDK.

### Integration with External Systems

  - [Federated Queries](../federated_query/index.md): Run queries on data in external systems (S3, ClickHouse, Postgres).
  - [Working with S3 via External Tables](../federated_query/s3/external_table.md): Read and write Parquet/CSV data in Object Storage.

## Data Processing and Transformation (ETL/ELT)

Query language and integration with orchestration tools.

### YQL Query Language

  - [Full Reference for YQL](../../yql/reference/index.md): Syntax, functions, and operators.
  - [Date and Time Functions](../../yql/reference/udf/list/datetime.md): Complete list and typical use cases.
  - [JSON Functions](../../yql/reference/builtins/json.md): Extracting data from JSON documents.

### Pipeline Building Tools

   - [dbt (Data Build Tool) Integration](../../integrations/migration/dbt.md): Managing ELT pipelines with SQL.
   - [Apache Airflow Integration](../../integrations/orchestration/airflow.md): Orchestrating complex ETL/ELT processes.

## Application Development & Integration (Development & SDKs)

Tools for application developers.

  - [{{ydb-short-name}} SDK Overview](../../reference/ydb-sdk/index.md): Native SDKs for Go, Python, Java, C++, Node.js.
  - [JDBC driver](../../reference/languages-and-apis/jdbc-driver/index.md): Standard connectivity for the Java ecosystem.
  - [{{ydb-short-name}} CLI](../../reference/ydb-cli/index.md): Command-line tool for administration and query execution.

## Data Analysis and Visualization

Integration with end-user tools.

### BI Tools

  - [Apache Superset](../../integrations/visualization/superset.md)
  - [Grafana](../../integrations/visualization/grafana.md)
  - [Yandex DataLens](../../integrations/visualization/datalens.md)

### Data Science Tools

  - [Jupyter Notebooks](../../integrations/gui/jupyter.md): Running YQL queries and interactive data analysis.

## Operations & Performance Management

Administration, monitoring, security, and optimization.

### Performance Management

  - [Query Plan Analysis (EXPLAIN)](../../dev/query-plans-optimization.md): How to understand query execution plans and identify bottlenecks.
  - [Cost-Based Optimizer](../optimizer.md): Overview of how the query planner works.

### Monitoring and Diagnostics

  - [Embedded UI](../../reference/embedded-ui/index.md): Web interface for cluster monitoring and diagnostics.
  - [Metrics Reference](../../reference/observability/metrics/index.md): Full list of metrics for monitoring systems.
  - [Ready-to-use Grafana Dashboards](../../reference/observability/metrics/grafana-dashboards.md): Templates for quick monitoring setup.

### Security and Fault Tolerance

  - [Authentication and Authorization](../../security/authentication.md): User access configuration, including LDAP support.
