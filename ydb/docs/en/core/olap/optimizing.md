# Optimizing analytical queries

1. **Selecting the right primary key**
   * Use columns frequently involved in filtering
   * Consider column order to optimize range queries

2. **Optimizing the data schema**
   * Use [column-based storage](../concepts/datamodel/table.md#column-oriented-tables) for analytical tables
   * Choose data types carefully to save space
   * Implement [partitioning](../concepts/datamodel/table.md#olap-tables-partitioning) for large (larger than 2Gb) analytical tables

3. **Query optimization**
   * Retrieve only necessary columns
   * Use appropriate [indexes](../concepts/secondary_indexes.md)
   * Formulate filter conditions effectively

{% include [olap-links](_includes/olap-links.md) %}