# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт — перенос данных из **PostgreSQL** в {{ ydb-short-name }} с помощью [ydb-importer](../../../../integrations/data-migration/import-jdbc.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Прямое JDBC-подключение к PostgreSQL и параллельная загрузка в {{ ydb-short-name }} через Bulk Upsert.

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [пример конфигурации PostgreSQL](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-postgres.xml).

### Системные требования

* JDK 8+ (OpenJDK 8+)
* Архив [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* JDBC-драйвер PostgreSQL (`postgresql-*.jar` в каталоге `lib/`)

### Шаг 1. Распакуйте дистрибутив и положите JDBC-драйвер

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp postgresql-*.jar ~/ydb-importer/lib/
```

### Шаг 2. Подготовьте `postgres-import.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers><pool size="4"/></workers>
    <source type="postgresql">
        <jdbc-class>org.postgresql.Driver</jdbc-class>
        <jdbc-url>jdbc:postgresql://pg-host:5432/mydb</jdbc-url>
        <username>user</username>
        <password>password</password>
    </source>
    <target type="ydb">
        <connection-string>grpc://localhost:2136?database=/local</connection-string>
        <auth-mode>NONE</auth-mode>
        <replace-existing>false</replace-existing>
        <load-data>true</load-data>
    </target>
    <table-options name="default">
        <table-name-format>pg/${schema}/${table}</table-name-format>
    </table-options>
    <table-map options="default">
        <include-schemas regexp="true">public</include-schemas>
    </table-map>
</ydb-importer>
```

### Шаг 3. Запустите импорт

```bash
cd ~/ydb-importer
./ydb-importer.sh postgres-import.xml
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
