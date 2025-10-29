# Video Game Sales

{% include [intro](_includes/intro.md) %}

Data on video game sales.

**Source**: [Kaggle - Video Game Sales](https://www.kaggle.com/datasets/gregorut/videogamesales)

**Size**: 1.36 MB

## Loading Example

1. Download and unzip the `vgsales.csv` file from Kaggle.

2. Create a table in {{ ydb-short-name }} using one of the following methods:

    {% list tabs %}

    - Embedded UI

      For more information on [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

      ```sql
      CREATE TABLE `vgsales` (
          `Rank` Uint64 NOT NULL,
          `Name` Text NOT NULL,
          `Platform` Text NOT NULL,
          `Year` Text NOT NULL,
          `Genre` Text NOT NULL,
          `Publisher` Text NOT NULL,
          `NA_Sales` Double NOT NULL,
          `EU_Sales` Double NOT NULL,
          `JP_Sales` Double NOT NULL,
          `Other_Sales` Double NOT NULL,
          `Global_Sales` Double NOT NULL,
          PRIMARY KEY (`Rank`)
      )
      WITH (
          STORE = COLUMN
      );
      ```

    - YDB CLI

      ```bash
      ydb sql -s \
      'CREATE TABLE `vgsales` (
          `Rank` Uint64 NOT NULL,
          `Name` Text NOT NULL,
          `Platform` Text NOT NULL,
          `Year` Text NOT NULL,
          `Genre` Text NOT NULL,
          `Publisher` Text NOT NULL,
          `NA_Sales` Double NOT NULL,
          `EU_Sales` Double NOT NULL,
          `JP_Sales` Double NOT NULL,
          `Other_Sales` Double NOT NULL,
          `Global_Sales` Double NOT NULL,
          PRIMARY KEY (`Rank`)
      )
      WITH (
          STORE = COLUMN
      );'
      ```

    {% endlist %}

3. Execute the import command:

    ```bash
    ydb import file csv --header --null-value "" --path vgsales vgsales.csv
    ```

## Analytical Query Example

To identify the publisher with the highest average game sales in North America, execute the query:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT
      Publisher,
      AVG(NA_Sales) AS average_na_sales
  FROM vgsales
  GROUP BY Publisher
  ORDER BY average_na_sales DESC
  LIMIT 1;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT
      Publisher,
      AVG(NA_Sales) AS average_na_sales
  FROM vgsales
  GROUP BY Publisher
  ORDER BY average_na_sales DESC
  LIMIT 1;'
  ```

{% endlist %}

Result:

```raw
┌───────────┬──────────────────┐
│ Publisher │ average_na_sales │
├───────────┼──────────────────┤
│ "Palcom"  │ 3.38             │
└───────────┴──────────────────┘
```

This query helps find the publisher with the greatest success in North America by average sales.