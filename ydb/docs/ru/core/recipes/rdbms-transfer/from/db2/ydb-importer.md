# Перенос данных из IBM Db2 в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт: **IBM Db2** → {{ ydb-short-name }} через [ydb-importer](../../tools/ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [ydb-importer](../../_includes/tools/ydb-importer-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Db2)

```bash
db2 "SELECT 1 FROM SYSIBM.SYSDUMMY1"
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [sample-db2.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-db2.xml).

### Требования

* JDK 8+
* `db2jcc4.jar` в `lib/`

```xml
<source type="db2">
    <jdbc-class>com.ibm.db2.jcc.DB2Driver</jdbc-class>
    <jdbc-url>jdbc:db2://db2-host:50000/SAMPLE</jdbc-url>
    <username>db2inst1</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh db2-import.xml
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
db2 "SELECT COUNT(*) FROM customers"
```
