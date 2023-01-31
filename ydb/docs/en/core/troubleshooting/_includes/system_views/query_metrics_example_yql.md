Top 10 queries for the last 6 hours by the total number of rows updated per minute:

> ```sql
> SELECT
>     SumUpdateRows,
>     Count,
>     QueryText,
>     IntervalEnd
> FROM `.sys/query_metrics_one_minute`
> ORDER BY SumUpdateRows DESC LIMIT 10
> ```

Recent queries that read the most bytes per minute:

> ```sql
> SELECT
>     IntervalEnd,
>     SumReadBytes,
>     MinReadBytes,
>     SumReadBytes / Count as AvgReadBytes,
>     MaxReadBytes,
>     QueryText
> FROM `.sys/query_metrics_one_minute`
> WHERE SumReadBytes > 0
> ORDER BY IntervalEnd DESC, SumReadBytes DESC
> LIMIT 100
> ```
