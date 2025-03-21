DEFINE SUBQUERY $q($p) AS

SELECT * FROM local_solomon.my_project WITH (
  program = $p,
  `downsampling.aggregation` = "SUM",
  `downsampling.grid_interval` = "25",
  `downsampling.fill` = "PREVIOUS"
);

END DEFINE;

SELECT * FROM $q(@@{}@@);
