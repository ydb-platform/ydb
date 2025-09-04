SELECT value, l2 FROM local_solomon.my_project WITH (
  program = @@{}@@,
  from = "1970-01-01T00:00:01Z",
  to = "1970-01-02T00:00:01Z",
  labels = "label1 as l1, label2 as l2"
);
