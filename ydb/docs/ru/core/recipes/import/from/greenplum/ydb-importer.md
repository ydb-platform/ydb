# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт: **Greenplum** → {{ ydb-short-name }} через [ydb-importer](../../../../integrations/data-migration/import-jdbc.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [sample-greenplum.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-greenplum.xml).

Greenplum подключается через **PostgreSQL JDBC-драйвер**; в конфигурации указывается `source type="greenplum"`.

### Требования

* JDK 8+
* `postgresql-*.jar` в `lib/`
* Сетевой доступ с хоста ydb-importer к master/segment (порт 5432 или ваш)

```xml
<source type="greenplum">
    <jdbc-class>org.postgresql.Driver</jdbc-class>
    <jdbc-url>jdbc:postgresql://gp-master:5432/mydb</jdbc-url>
    <username>gpadmin</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh greenplum-import.xml
```

{% note info %}

Для выборочного импорта используйте `table-map` / `table-ref` в XML — см. [формат настроек](../../../../integrations/data-migration/import-jdbc.md#config). Альтернатива без JDBC на стороне клиента — [федеративные запросы](federated-queries.md).

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/mydb" -c "SELECT COUNT(*) FROM customers"
```
