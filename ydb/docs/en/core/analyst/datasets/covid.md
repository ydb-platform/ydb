# COVID-19 Open Research Dataset

{% include [intro](_includes/intro.md) %}

An open dataset of COVID-19 research.

**Source**: [Kaggle - COVID-19 Open Research Dataset Challenge](https://www.kaggle.com/datasets/allen-institute-for-ai/CORD-19-research-challenge?select=metadata.csv)

**Size**: 1.65 GB (metadata.csv file)

## Loading Example

1. Download and unzip the `metadata.csv` file from Kaggle.

2. The dataset includes completely identical rows. Since YDB requires unique primary key values, add a new column named `row_id` to the file, where the key value will be equal to the row number in the original file. This prevents the removal of duplicate data. This operation can be carried out using the awk command:

    ```bash
    awk 'NR==1 {print "row_id," \$0; next} {print NR-1 "," \$0}' metadata.csv > temp.csv && mv temp.csv metadata.csv
    ```

3. Create a table in {{ ydb-short-name }} using one of the following methods:

    {% list tabs %}

    - Embedded UI

      For more information on [Embedded UI](../../reference/embedded-ui/ydb-monitoring).

      ```sql
      CREATE TABLE `covid_research` (
          `row_id` Uint64 NOT NULL,
          `cord_uid` Text NOT NULL,
          `sha` Text NOT NULL,
          `source_x` Text NOT NULL,
          `title` Text NOT NULL,
          `doi` Text NOT NULL,
          `pmcid` Text NOT NULL,
          `pubmed_id` Text NOT NULL,
          `license` Text NOT NULL,
          `abstract` Text NOT NULL,
          `publish_time` Text NOT NULL,
          `authors` Text NOT NULL,
          `journal` Text NOT NULL,
          `mag_id` Text,
          `who_covidence_id` Text,
          `arxiv_id` Text,
          `pdf_json_files` Text NOT NULL,
          `pmc_json_files` Text NOT NULL,
          `url` Text NOT NULL,
          `s2_id` Uint64,
          PRIMARY KEY (`row_id`)
      )
      WITH (
          STORE = COLUMN
      );
      ```

    - YDB CLI

      ```bash
      ydb sql -s \
      'CREATE TABLE `covid_research` (
          `row_id` Uint64 NOT NULL,
          `cord_uid` Text NOT NULL,
          `sha` Text NOT NULL,
          `source_x` Text NOT NULL,
          `title` Text NOT NULL,
          `doi` Text NOT NULL,
          `pmcid` Text NOT NULL,
          `pubmed_id` Text NOT NULL,
          `license` Text NOT NULL,
          `abstract` Text NOT NULL,
          `publish_time` Text NOT NULL,
          `authors` Text NOT NULL,
          `journal` Text NOT NULL,
          `mag_id` Text,
          `who_covidence_id` Text,
          `arxiv_id` Text,
          `pdf_json_files` Text NOT NULL,
          `pmc_json_files` Text NOT NULL,
          `url` Text NOT NULL,
          `s2_id` Uint64,
          PRIMARY KEY (`row_id`)
      )
      WITH (
          STORE = COLUMN
      );'
      ```

    {% endlist %}

4. Execute the import command:

    ```bash
    ydb import file csv --header --null-value "" --path covid_research metadata.csv
    ```

## Analytical Query Example

Run a query to determine the journals with the highest number of publications:

{% list tabs %}

- Embedded UI

  ```sql
  SELECT
      journal,
      COUNT(*) AS publication_count
  FROM covid_research
  WHERE journal IS NOT NULL AND journal != ''
  GROUP BY journal
  ORDER BY publication_count DESC
  LIMIT 10;
  ```

- YDB CLI

  ```bash
  ydb sql -s \
  'SELECT
      journal,
      COUNT(*) AS publication_count
  FROM covid_research
  WHERE journal IS NOT NULL AND journal != ""
  GROUP BY journal
  ORDER BY publication_count DESC
  LIMIT 10;'
  ```

{% endlist %}

Result:

```raw
┌───────────────────────────────────┬───────────────────┐
│ journal                           │ publication_count │
├───────────────────────────────────┼───────────────────┤
│ "PLoS One"                        │ 9953              │
├───────────────────────────────────┼───────────────────┤
│ "bioRxiv"                         │ 8961              │
├───────────────────────────────────┼───────────────────┤
│ "Int J Environ Res Public Health" │ 8201              │
├───────────────────────────────────┼───────────────────┤
│ "BMJ"                             │ 6928              │
├───────────────────────────────────┼───────────────────┤
│ "Sci Rep"                         │ 5935              │
├───────────────────────────────────┼───────────────────┤
│ "Cureus"                          │ 4212              │
├───────────────────────────────────┼───────────────────┤
│ "Reactions Weekly"                │ 3891              │
├───────────────────────────────────┼───────────────────┤
│ "Front Psychol"                   │ 3541              │
├───────────────────────────────────┼───────────────────┤
│ "BMJ Open"                        │ 3515              │
├───────────────────────────────────┼───────────────────┤
│ "Front Immunol"                   │ 3442              │
└───────────────────────────────────┴───────────────────┘
```