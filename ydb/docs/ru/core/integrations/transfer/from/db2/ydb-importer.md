# Перенос данных из IBM Db2 в {{ ydb-short-name }} с помощью ydb-importer

Прямое JDBC-подключение к IBM Db2 и параллельная загрузка в {{ ydb-short-name }} через Bulk Upsert.

Подробнее про инструмент: [импорт из JDBC](../../data-migration/import-jdbc.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| IBM Db2 | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (IBM Db2)

```bash
db2 "SELECT 1 FROM SYSIBM.SYSDUMMY1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [импорт из JDBC](../../data-migration/import-jdbc.md), [sample-db2.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-db2.xml).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
db2 "SELECT COUNT(*) FROM customers"
```
