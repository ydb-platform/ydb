SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z",

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
