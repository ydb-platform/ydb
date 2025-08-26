SELECT * FROM local_solomon.hist WITH (
  program = @@histogram_percentile(95, {})@@,
  from = "1970-01-01T00:00:01Z",
  to = "1970-01-02T00:00:01Z"
);
