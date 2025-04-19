# Использование JDBC-драйвера с Maven

Рекомендованный способ использования JDBC-драйвера для {{ ydb-short-name }} в проекте — это добавить драйвер как зависимость в Maven. Укажите JDBC-драйвер для {{ ydb-short-name }} в секции `dependencies` файла `pom.xml`:

```xml
<dependencies>
    <dependency>
        <groupId>tech.ydb.jdbc</groupId>
        <artifactId>ydb-jdbc-driver</artifactId>
        <version><!-- актуальная версия --></version>
    </dependency>
</dependencies>
```