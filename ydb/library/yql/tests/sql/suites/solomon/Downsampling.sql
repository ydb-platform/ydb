SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z",
  `downsampling.disabled` = "true",
  `downsampling.aggregation` = "SUM",
  `downsampling.grid_interval` = "25",
  `downsampling.fill` = "PREVIOUS"
);
