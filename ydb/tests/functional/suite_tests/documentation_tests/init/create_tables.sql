CREATE TABLE `test_table_4` (
    a Uint64,
    b Uint64,
    c Float,
    PRIMARY KEY (a, b)
);

CREATE TABLE `test_table_5` (
  a Uint64 NOT NULL,
  b Timestamp NOT NULL,
  c Float,
  PRIMARY KEY (a, b)
)
PARTITION BY HASH(b)
WITH (
  STORE = COLUMN
);
