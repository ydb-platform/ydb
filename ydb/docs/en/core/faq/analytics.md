# Questions and answers about analytics in {{ ydb-short-name }}

## Can {{ ydb-short-name }} be used for analytical workloads (OLAP)?

Yes, it can. If this is the primary type of workload for a given table, make sure it is [column-oriented](../concepts/datamodel/table.md#column-oriented-tables).

## How to choose between row-oriented and column-oriented tables?

Similarly to choosing between transactional (OLTP) and analytical (OLAP) database management systems, this question comes to a number of trade-offs that need to be considered:

* **What's the main use case for the table?** For mostly transactional (OLTP) workloads, use [row-oriented tables](../concepts/datamodel/table.md#row-oriented-tables). For analytical workloads (OLAP), use [column-oriented tables](../concepts/datamodel/table.md#column-oriented-tables). Transactional workloads are characterized by a high rate of queries affecting a small number of rows each. Analytical workloads are characterized by processing large volumes of data to produce relatively small query results.
* **How is the table modified?** As a rule of thumb, row-oriented tables work better when data is frequently modified in place, while column-oriented tables work better when data is mostly appended by adding new rows. Thus, row-oriented tables usually reflect the current state of a dataset, while column-oriented tables often store a history of some sort of immutable events.
* **Which features are needed?** Even though {{ ydb-short-name }} strives for feature parity between row-oriented and column-oriented tables, there might be current limitations to consider. Check the documentation for details on specific features intended to be used with a given table.

Unlike most other database management systems, {{ ydb-short-name }} supports both row-oriented and column-oriented tables in the same [database](../concepts/glossary.md#database). However, keep in mind that transactional and analytical workloads have different resource consumption patterns and might affect each other when the cluster is overloaded.