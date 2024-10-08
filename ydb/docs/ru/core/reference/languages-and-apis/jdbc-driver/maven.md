# Использование JDBC-драйвера с Maven

Рекомендованный способ использования JDBC-драйвера для {{ ydb-short-name }} в проекте — это добавить драйвер как зависимость в Maven. Укажите JDBC-драйвер для {{ ydb-short-name }} в секции `dependencies` файла `pom.xml`:

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