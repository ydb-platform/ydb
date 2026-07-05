# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт — перенос данных из **Microsoft SQL Server** в {{ ydb-short-name }} с помощью [ydb-importer](../../../../integrations/data-migration/import-jdbc.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [sample-mssql.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mssql.xml).

### Требования

* JDK 8+
* `mssql-jdbc-*.jar` в `lib/` дистрибутива ydb-importer

```xml
<source type="mssql">
    <jdbc-class>com.microsoft.sqlserver.jdbc.SQLServerDriver</jdbc-class>
    <jdbc-url>jdbc:sqlserver://mssql-host:1433;databaseName=mydb;encrypt=true;trustServerCertificate=true</jdbc-url>
    <username>user</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh mssql-import.xml
```

{% note info %}

Пространственные типы SQL Server не поддерживаются ydb-importer.

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
