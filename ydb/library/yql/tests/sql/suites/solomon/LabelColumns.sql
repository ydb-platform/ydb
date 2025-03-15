SELECT * FROM local_solomon.my_project WITH (
  program = @@{}@@,
  labels = "downsampling.disabled, downsampling.fill, project, downsampling.gridMillis"
);
