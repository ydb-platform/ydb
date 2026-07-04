# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью ydb-importer

Прямое JDBC-подключение к Microsoft SQL Server и параллельная загрузка в {{ ydb-short-name }} через Bulk Upsert.

Подробнее про инструмент: [импорт из JDBC](../../data-migration/import-jdbc.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Microsoft SQL Server | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
| Кодировка | UTF-8 для текстовых данных |

### Установка {{ ydb-short-name }} CLI

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../../reference/ydb-cli/install.md).

### Проверка подключения к {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd или SSMS: SELECT 1
```

---

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../data-migration/import-jdbc.md), [sample-mssql.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mssql.xml).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
