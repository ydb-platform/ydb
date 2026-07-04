# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью mysql2ydb

Специализированный инструмент для копии MySQL «один к одному», с возобновлением и поддержкой больших таблиц.

Подробнее про инструмент: [импорт из MySQL](../../data-migration/import-mysql.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| MySQL / MariaDB | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Специализированный инструмент для копии MySQL «один к одному», с возобновлением и поддержкой больших таблиц.

Подробнее: [импорт из MySQL](../../data-migration/import-mysql.md), [репозиторий mysql-ydb-importer](https://github.com/ydb-platform/mysql-ydb-importer).

### Требования

* Go (версия из `go.mod` репозитория)
* Файл `~/.my.cnf` или флаг `-mysql`

### Шаг 1. Сборка

```bash
git clone https://github.com/ydb-platform/mysql-ydb-importer.git
cd mysql-ydb-importer
go build -o mysql2ydb ./cmd/mysql2ydb
```

### Шаг 2. Настройте `~/.my.cnf`

```ini
[client]
user = myuser
password = mypass
host = mysql-host
port = 3306
database = mydb
```

### Шаг 3. Запуск

Только данные (схема уже в {{ ydb-short-name }}):

```bash
./mysql2ydb -ydb "grpc://localhost:2136" -data-only -batch-size 10000
```

Схема и данные:

```bash
./mysql2ydb -mysql "user:pass@tcp(mysql-host:3306)/mydb" \
  -ydb "grpc://localhost:2136" -batch-size 10000
```

{% note warning %}

У каждой таблицы MySQL должен быть `PRIMARY KEY` (или подготовьте схему вручную и используйте `-data-only`).

{% endnote %}

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
