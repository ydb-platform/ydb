# Netflix Movies and TV Shows

{% include [intro](_includes/intro.md) %}

Данные о фильмах и сериалах на платформе Netflix.

**Источник**: [Kaggle - Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)

**Размер**: 3.4 MB

## Пример загрузки

1. Скачайте и разархивируйте файл `netflix_titles.csv` с Kaggle

2. Создайте таблицу в {{ ydb-short-name }} одним из следующих способов:

{% list tabs %}

- Embedded UI

  Подробнее про [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

  ```sql
  CREATE TABLE `netflix` (
      `show_id` Text NOT NULL,
      `type` Text NOT NULL,
      `title` Text NOT NULL,
      `director` Text NOT NULL,
      `cast` Text,
      `country` Text NOT NULL,
      `date_added` Text NOT NULL,
      `release_year` Uint64 NOT NULL,
      `rating` Text NOT NULL,
      `duration` Text NOT NULL,
      `listed_in` Text NOT NULL,
      `description` Text NOT NULL,
      PRIMARY KEY (`show_id`)
  )
  WITH (
      STORE = COLUMN
  );
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'CREATE TABLE `netflix` (
      `show_id` Text NOT NULL,
      `type` Text NOT NULL,
      `title` Text NOT NULL,
      `director` Text NOT NULL,
      `cast` Text,
      `country` Text NOT NULL,
      `date_added` Text NOT NULL,
      `release_year` Uint64 NOT NULL,
      `rating` Text NOT NULL,
      `duration` Text NOT NULL,
      `listed_in` Text NOT NULL,
      `description` Text NOT NULL,
      PRIMARY KEY (`show_id`)
  )
  WITH (
      STORE = COLUMN
  );'
  ```

{% endlist %}

3. Выполните команду импорта:

```bash
ydb import file csv --header --null-value "" --path netflix netflix_titles.csv
```

## Пример аналитического запроса

Определим три страны, из которых было добавлено больше всего контента на Netflix в 2020 году:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT
      country,
      COUNT(*) AS count
  FROM netflix
  WHERE
      CAST(SUBSTRING(CAST(date_added AS String), 7, 4) AS Int32) = 2020
      AND date_added IS NOT NULL
  GROUP BY country
  ORDER BY count DESC
  LIMIT 3;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT
      country,
      COUNT(*) AS count
  FROM netflix
  WHERE
      CAST(SUBSTRING(CAST(date_added AS String), 7, 4) AS Int32) = 2020
      AND date_added IS NOT NULL
  GROUP BY country
  ORDER BY count DESC
  LIMIT 3;'
  ```

{% endlist %}

Результат:

```raw
┌─────────────────┬───────┐
│ country         │ count │
├─────────────────┼───────┤
│ "United States" │ 22    │
├─────────────────┼───────┤
│ ""              │ 7     │
├─────────────────┼───────┤
│ "Canada"        │ 3     │
└─────────────────┴───────┘
```