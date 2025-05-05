DEFINE SUBQUERY $q($p) AS

SELECT * FROM local_solomon.my_project WITH (
  program = $p,
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z",
  `downsampling.aggregation` = "SUM",
  `downsampling.grid_interval` = "25",
  `downsampling.fill` = "PREVIOUS"
);

END DEFINE;

SELECT * FROM $q(@@{}@@);
