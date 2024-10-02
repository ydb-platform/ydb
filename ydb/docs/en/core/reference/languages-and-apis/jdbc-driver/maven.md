# Using the JDBC driver with Maven

The recommended way to use the {{ ydb-short-name }} JDBC driver in your project is to consume it from Maven.
Specify the {{ ydb-short-name }} JDBC driver in the `dependencies` section:

```xml
<dependencies>
    <!-- Base version -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version>2.2.9</version>
    </dependency>

    <!-- Shaded version with included dependencies -->
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver-shaded</artifactId>
        <version>2.2.9</version>
    </dependency>
</dependencies>
```