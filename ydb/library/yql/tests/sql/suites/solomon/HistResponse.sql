SELECT * FROM local_solomon.hist WITH (
  program = @@histogram_percentile(95, {})@@,
  from = "2023-12-08T14:40:39Z",
  to = "2023-12-08T14:45:39Z"
);
