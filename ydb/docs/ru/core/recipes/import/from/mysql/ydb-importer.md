# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью ydb-importer

Пошаговый рецепт — перенос данных из **MySQL / MariaDB** в {{ ydb-short-name }} с помощью [ydb-importer](../../../../integrations/data-migration/import-jdbc.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../../../integrations/data-migration/import-jdbc.md), [sample-mysql.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mysql.xml).

### Требования

* JDK 8+
* [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* `mysql-connector-j-*.jar` или `mariadb-java-client-*.jar` в `lib/`

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp mysql-connector-j-*.jar ~/ydb-importer/lib/
```

Минимальный фрагмент конфигурации:

```xml
<source type="mysql">
    <jdbc-class>com.mysql.cj.jdbc.Driver</jdbc-class>
    <jdbc-url>jdbc:mysql://mysql-host:3306/mydb</jdbc-url>
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
./ydb-importer.sh mysql-import.xml
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
