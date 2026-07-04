# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **MySQL / MariaDB** → {{ ydb-short-name }} через [CLI import file](../../tools/cli-import-file.md).

## Подготовка {#prerequisites}

{% include notitle [CLI import file](../../_includes/tools/cli-import-file-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные в CSV

```bash
mysql -h mysql-host -u user -p mydb \
  -e "SELECT id, amount FROM orders" \
  --batch --raw --skip-column-names \
  | sed 's/\t/,/g' > /tmp/orders.csv
```

Для надёжного CSV с заголовком используйте `INTO OUTFILE` (если разрешено на сервере) или клиентские утилиты (`mydumper`, DBeaver export).

### Шаг 3. Импортируйте в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/orders --header --columns id,amount /tmp/orders.csv
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
