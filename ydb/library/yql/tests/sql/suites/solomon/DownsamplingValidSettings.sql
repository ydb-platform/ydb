SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,

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
