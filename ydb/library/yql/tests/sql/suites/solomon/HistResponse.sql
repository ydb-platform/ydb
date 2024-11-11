SELECT * FROM local_solomon.hist WITH (
  program = @@histogram_percentile(95, {})@@
);
