# Quick start

1. Download the [JDBC driver for YDB](https://github.com/ydb-platform/ydb-jdbc-driver/releases).

1. Copy the .jar file to the directory specified in the `classpath` environment variable or load the .jar file in your IDE.

1. Connect to {{ ydb-short-name }}. JDBC URL examples:

    {% include notitle [examples](_includes/jdbc-url-examples.md) %}

1. Execute queries, for example, [YdbDriverExampleTest.java](jdbc/src/test/java/tech/ydb/jdbc/YdbDriverExampleTest.java).
