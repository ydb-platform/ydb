  Топ-10 запросов за последние 6 часов по общему количеству записанных строк в минутном интервале:
  
  > ```sql
  > SELECT
  >     SumUpdateRows,
  >     Count,
  >     QueryText,
  >     IntervalEnd
  > FROM `.sys/query_metrics_one_minute`
  > ORDER BY SumUpdateRows DESC LIMIT 10
  > ```

  Недавние запросы, прочитавшие больше всего байт за минуту:
  
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