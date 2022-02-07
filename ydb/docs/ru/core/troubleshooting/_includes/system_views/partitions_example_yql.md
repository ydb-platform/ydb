  Топ-5 самых загруженных партиций среди всех таблиц базы данных
  ```sql
  SELECT
      Path,
      PartIdx,
      CPUCores
  FROM `/cluster/path/to/database/.sys/partition_stats`
  ORDER BY CPUCores DESC
  LIMIT 5
  ```

  Список таблиц базы с размерами и нагрузкой в моменте
  ```sql
  SELECT
      Path,
      COUNT(*) as Partitions,
      SUM(RowCount) as Rows,
      SUM(DataSize) as Size,
      SUM(CPUCores) as CPU
  FROM `/cluster/path/to/database/.sys/partition_stats`
  GROUP BY Path
  ```