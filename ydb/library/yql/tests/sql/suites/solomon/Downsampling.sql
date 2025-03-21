SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  `downsampling.aggregation` = "SUM",
  `downsampling.grid_interval` = "25",
  `downsampling.fill` = "PREVIOUS"
);
