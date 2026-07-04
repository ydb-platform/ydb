# Перенос данных из IBM Informix в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт: **IBM Informix** → {{ ydb-short-name }} через [ydb-importer](../../tools/ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [ydb-importer](../../_includes/tools/ydb-importer-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Informix)

```bash
# dbaccess stores_demo -
# SELECT 1 FROM systables WHERE tabid=1;
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [sample-informix.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-informix.xml).

### Требования

* JDK 8+
* `ifxjdbc.jar` в `lib/` дистрибутива ydb-importer
* Имя `INFORMIXSERVER` и порт — как в `sqlhosts` вашей инсталляции

```xml
<source type="informix">
    <jdbc-class>com.informix.jdbc.IfxDriver</jdbc-class>
    <jdbc-url>jdbc:informix-sqli://ifx-host:9088/stores_demo:INFORMIXSERVER=informix</jdbc-url>
    <username>informix</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh informix-import.xml
```

{% note warning %}

Объектные типы данных Informix не поддерживаются ydb-importer. См. [ограничения](../../../../integrations/data-migration/import-jdbc.md#limitations).

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
# dbaccess stores_demo -
# SELECT COUNT(*) FROM customer;
```
