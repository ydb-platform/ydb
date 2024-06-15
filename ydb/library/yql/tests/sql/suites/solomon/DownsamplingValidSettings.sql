SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z",

  `downsampling.grid_aggregation` = "AVG",
  `downsampling.grid_aggregation` = "COUNT",
  `downsampling.grid_aggregation` = "DEFAULT_AGGREGATION",
  `downsampling.grid_aggregation` = "LAST",
  `downsampling.grid_aggregation` = "MAX",
  `downsampling.grid_aggregation` = "MIN",
  `downsampling.grid_aggregation` = "SUM",

  `downsampling.fill` = "NONE",
  `downsampling.fill` = "NULL",
  `downsampling.fill` = "PREVIOUS"
);
