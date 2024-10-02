# Использование JDBC-драйвера с Maven

Рекомендованный способ использования JDBC-драйвера для {{ ydb-short-name }} в проекте — это указать драйвер в Maven.

Добавьте JDBC-драйвер для {{ ydb-short-name }} в секцию `dependencies`:

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