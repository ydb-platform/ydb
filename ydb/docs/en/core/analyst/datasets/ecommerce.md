# E-Commerce Behavior Data

{% include [intro](_includes/intro.md) %}

User behavior data from a multi-category online store.

**Source**: [Kaggle - E-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data)

**Size**: 9 GB

## Loading Example

1. Download and unzip the `2019-Nov.csv` file from Kaggle.

2. The dataset includes completely identical rows. Since YDB requires unique primary key values, add a new column named `row_id` to the file, where the key value will be equal to the row number in the original file. This prevents the removal of duplicate data. This operation can be carried out using the awk command:

    ```bash
    awk 'NR==1 {print "row_id," \$0; next} {print NR-1 "," \$0}' 2019-Nov.csv > temp.csv && mv temp.csv 2019-Nov.csv
    ```

3. Create a table in {{ ydb-short-name }} using one of the following methods:

    {% list tabs %}

    - Embedded UI

      For more information on [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

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

4. Execute the import command:

    ```bash
    ydb import file csv --header --null-value "" --path ecommerce_table 2019-Nov.csv
    ```

## Analytical Query Example

Identify the most popular product categories on November 1, 2019:

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

Result:

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