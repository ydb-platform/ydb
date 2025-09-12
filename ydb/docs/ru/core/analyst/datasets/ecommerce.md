# E-Commerce Behavior Data

{% include [intro](_includes/intro.md) %}

Данные о поведении пользователей в мультикатегорийном интернет-магазине.

**Источник**: [Kaggle - E-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data)

**Размер**: 9 GB

## Пример загрузки

1. Скачайте и разархивируйте файл `2019-Nov.csv` с Kaggle

2. Датасет включает в себя полностью идентичные строки. Поскольку YDB требует указания уникальных значений первичного ключа, добавим в файл новую колонку под названием `row_id`, где значение ключа будет равно номеру строки в исходном файле. Это позволит предотвратить удаление повторяющихся данных. Эту операцию можно осуществить с помощью команды awk:

```bash
awk 'NR==1 {print "row_id," $0; next} {print NR-1 "," $0}' 2019-Nov.csv > temp.csv && mv temp.csv 2019-Nov.csv
```

3. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `ecommerce_table` (
      `row_id` Uint64 NOT NULL,
      `event_time` Text NOT NULL,
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `ecommerce_table` (
      `row_id` Uint64 NOT NULL,
      `event_time` Text NOT NULL,
      `event_type` Text NOT NULL,
      `product_id` Uint64 NOT NULL,
      `category_id` Uint64,
      `category_code` Text,
      `brand` Text,
      `price` Double NOT NULL,
      `user_id` Uint64 NOT NULL,
      `user_session` Text NOT NULL,
      PRIMARY KEY (`row_id`)
  )
  WITH (
      STORE = COLUMN,
      UNIFORM_PARTITIONS = 50
  );'
  ```

{% endlist %}

4. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path ecommerce_table 2019-Nov.csv
```

## Пример аналитического запроса

Определим самые популярные категории продуктов 1 ноября 2019 года:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT
      category_code,
      COUNT(*) AS view_count
  FROM ecommerce_table
  WHERE
      SUBSTRING(CAST(event_time AS String), 0, 10) = '2019-11-01'
      AND event_type = 'view'
  GROUP BY category_code
  ORDER BY view_count DESC
  LIMIT 10;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT
      category_code,
      COUNT(*) AS view_count
  FROM ecommerce_table
  WHERE
      SUBSTRING(CAST(event_time AS String), 0, 10) = "2019-11-01"
      AND event_type = "view"
  GROUP BY category_code
  ORDER BY view_count DESC
  LIMIT 10;'
  ```

{% endlist %}

Результат:

```raw
┌────────────────────────────────────┬────────────┐
│ category_code                      │ view_count │
├────────────────────────────────────┼────────────┤
│ null                               │ 453024     │
├────────────────────────────────────┼────────────┤
│ "electronics.smartphone"           │ 360650     │
├────────────────────────────────────┼────────────┤
│ "electronics.clocks"               │ 43581      │
├────────────────────────────────────┼────────────┤
│ "computers.notebook"               │ 40878      │
├────────────────────────────────────┼────────────┤
│ "electronics.video.tv"             │ 40383      │
├────────────────────────────────────┼────────────┤
│ "electronics.audio.headphone"      │ 37489      │
├────────────────────────────────────┼────────────┤
│ "apparel.shoes"                    │ 31013      │
├────────────────────────────────────┼────────────┤
│ "appliances.kitchen.washer"        │ 28028      │
├────────────────────────────────────┼────────────┤
│ "appliances.kitchen.refrigerators" │ 27808      │
├────────────────────────────────────┼────────────┤
│ "appliances.environment.vacuum"    │ 26477      │
└────────────────────────────────────┴────────────┘
```