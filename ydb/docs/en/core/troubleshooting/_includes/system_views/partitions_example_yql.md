Top 5 of most loaded partitions among all DB tables:

> ```sql
> SELECT
>     Path,
>     PartIdx,
>     CPUCores
> FROM `.sys/partition_stats`
> ORDER BY CPUCores DESC
> LIMIT 5
> ```

List of DB tables with in-flight sizes and loads:

> ```sql
> SELECT
>     Path,
>     COUNT(*) as Partitions,
>     SUM(RowCount) as Rows,
>     SUM(DataSize) as Size,
>     SUM(CPUCores) as CPU
> FROM `.sys/partition_stats`
> GROUP BY Path
> ```
