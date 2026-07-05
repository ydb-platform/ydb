# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт — перенос данных из **ClickHouse** в {{ ydb-short-name }} с помощью [ydb-importer](../../../../integrations/data-migration/import-jdbc.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [пример для ClickHouse](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-clickhouse.xml).

### Требования

* JDK 8+
* `clickhouse-jdbc-*.jar` в `lib/`
* Для таблиц без явного первичного ключа — задайте `key-column` в `table-ref` или примите синтетический ключ `ydb_synth_key` (см. [таблицы без PK](../../../../integrations/data-migration/import-jdbc.md#nopk))

```xml
<workers>
    <reader-pool size="4"/>
</workers>
<source type="clickhouse">
    <jdbc-class>com.clickhouse.jdbc.ClickHouseDriver</jdbc-class>
    <jdbc-url>jdbc:clickhouse://ch-host:8123/mydb</jdbc-url>
    <username>default</username>
    <password></password>
    <!-- У ClickHouse нет JDBC-курсора; размер порции чтения -->
    <fetch-size>200000</fetch-size>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh clickhouse-import.xml
```

{% note warning %}

ClickHouse не входит в список СУБД, для которых импорт описан в [import-jdbc.md](../../../../integrations/data-migration/import-jdbc.md#limitations) как штатно протестированный. Перед production проверьте маппинг типов и ключей на ваших таблицах. Для очень больших объёмов часто удобнее [Spark](spark.md) или экспорт в Parquet и [импорт через промежуточный CSV с помощью YDB CLI](cli-import-file.md).

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
