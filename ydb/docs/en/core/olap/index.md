# For Analysts

This section provides examples and recommendations for handling [analytical (OLAP) scenarios](../faq/analytics.md) in {{ ydb-short-name }}.

## This section includes the following materials:

* [Dataset import](datasets/index.md) - examples of importing and analyzing popular datasets

## Typical OLAP scenarios

1. **Batch analytics**
   * Aggregating large volumes of historical data
   * Generating reports over extended periods
   * Calculating statistics across entire datasets

2. **Interactive analytics**
   * Performing ad-hoc queries for data exploration
   * Building dynamic dashboards
   * Conducting drill-down analyses at various levels of detail

3. **Data preprocessing**
   * Cleaning and normalizing data
   * Enriching data from multiple sources
   * Preparing data for machine learning

## Recommendations for optimizing analytical queries

1. **Selecting the right primary key**
   * Use columns frequently involved in filtering
   * Consider column order to optimize range queries

2. **Optimizing the data schema**
   * Use [column-based storage](../concepts/datamodel/table.md#column-oriented-tables) for analytical tables
   * Choose data types carefully to save space
   * Implement [partitioning](../concepts/datamodel/table.md#olap-tables-partitioning) for large analytical tables

3. **Query optimization**
   * Retrieve only necessary columns
   * Use appropriate [indexes](../concepts/secondary_indexes.md)
   * Formulate filter conditions effectively

## Related sections

* [{#T}](../faq/analytics.md)
* [{#T}](../concepts/datamodel/table.md#column-oriented-tables)
* [{#T}](../yql/reference/builtins/aggregation.md)