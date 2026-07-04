# Перенос данных из Oracle Database в {{ ydb-short-name }} с помощью ydb-importer

Прямое JDBC-подключение к Oracle Database и параллельная загрузка в {{ ydb-short-name }} через Bulk Upsert.

Подробнее про инструмент: [импорт из JDBC](../../data-migration/import-jdbc.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Oracle Database | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

---

## Пошаговая инструкция {#steps}

Основной рекомендуемый способ прямого переноса из Oracle.

Подробнее: [импорт из JDBC](../../data-migration/import-jdbc.md), [sample-oracle.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-oracle.xml).

### Требования

* JDK 8+
* [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* `ojdbc*.jar` в каталоге `lib/`

### Шаг 1. Установка

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp ojdbc11.jar ~/ydb-importer/lib/
```

### Шаг 2. Конфигурация `oracle-import.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers><pool size="4"/></workers>
    <source type="oracle">
        <jdbc-class>oracle.jdbc.driver.OracleDriver</jdbc-class>
        <jdbc-url>jdbc:oracle:thin:@//ora-host:1521/ORCLPDB1</jdbc-url>
        <username>hr</username>
        <password>password</password>
    </source>
    <target type="ydb">
        <connection-string>grpc://localhost:2136?database=/local</connection-string>
        <auth-mode>NONE</auth-mode>
        <load-data>true</load-data>
        <max-batch-rows>1000</max-batch-rows>
    </target>
    <table-options name="default">
        <table-name-format>ora/${schema}/${table}</table-name-format>
        <blob-name-format>ora/${schema}/${table}_${field}</blob-name-format>
    </table-options>
    <table-map options="default">
        <include-schemas regexp="true">HR</include-schemas>
    </table-map>
</ydb-importer>
```

### Шаг 3. Запуск

```bash
cd ~/ydb-importer
./ydb-importer.sh oracle-import.xml
```

{% note warning %}

Вложенные таблицы Oracle (nested tables) не поддерживаются. BLOB выносятся в отдельные таблицы — см. [ограничения ydb-importer](../../data-migration/import-jdbc.md#blob).

{% endnote %}

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с источником:

```bash
SELECT COUNT(*) FROM hr.employees;
```
