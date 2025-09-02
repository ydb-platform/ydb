DEFINE SUBQUERY $q($p) AS

SELECT * FROM local_solomon.my_project WITH (
  program = $p,
  from = "1970-01-01T00:00:01Z",
  to = "1970-01-02T00:00:01Z",
  `downsampling.aggregation` = "SUM",
  `downsampling.grid_interval` = "25",
  `downsampling.fill` = "PREVIOUS"
);

END DEFINE;

SELECT * FROM $q(@@{}@@);
