SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  from = "1970-01-01T00:00:01Z",
  to = "1970-01-02T00:00:01Z",

  `downsampling.aggregation` = "AVG",
  `downsampling.aggregation` = "COUNT",
  `downsampling.aggregation` = "DEFAULT_AGGREGATION",
  `downsampling.aggregation` = "LAST",
  `downsampling.aggregation` = "MAX",
  `downsampling.aggregation` = "MIN",
  `downsampling.aggregation` = "SUM",

  `downsampling.fill` = "NONE",
  `downsampling.fill` = "NULL",
  `downsampling.fill` = "PREVIOUS"
);
