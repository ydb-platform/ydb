# Netflix Movies and TV Shows

{% include [intro](_includes/intro.md) %}

Data on movies and TV shows available on Netflix.

**Source**: [Kaggle - Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)

**Size**: 3.4 MB

## Loading Example

1. Download and unzip the `netflix_titles.csv` file from Kaggle.

2. Create a table in {{ ydb-short-name }} using one of the following methods:

    {% list tabs %}

    - Embedded UI

      For more information on [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

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

3. Execute the import command:

    ```bash
    ydb import file csv --header --null-value "" --path netflix netflix_titles.csv
    ```

## Analytical Query Example

Identify the top three countries with the most content added to Netflix in 2020:

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

Result:

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