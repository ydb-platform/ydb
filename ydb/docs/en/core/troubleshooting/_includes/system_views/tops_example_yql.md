Top queries by execution time for the last minute when queries were made:

> ```sql
> PRAGMA AnsiInForEmptyOrNullableItemsCollections;
> $last = (
>     SELECT
>         MAX(IntervalEnd)
>     FROM `.sys/top_queries_by_duration_one_minute`
> );
> SELECT
>     IntervalEnd,
>     Rank,
>     QueryText,
>     Duration
> FROM `.sys/top_queries_by_duration_one_minute`
> WHERE IntervalEnd IN $last
> ```

Queries that read the most bytes, broken down by minute:

> ```sql
> SELECT
>     IntervalEnd,
>     QueryText,
>     ReadBytes,
>     ReadRows,
>     Partitions
> FROM `.sys/top_queries_by_read_bytes_one_minute`
> WHERE Rank = 1
> ```
