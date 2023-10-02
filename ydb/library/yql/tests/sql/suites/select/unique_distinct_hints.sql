/* postgres can not */
USE plato;

INSERT INTO Output1 WITH TRUNCATE SELECT /*+ unique() */ * FROM Input;
INSERT INTO Output2 WITH TRUNCATE SELECT /*+ distinct() */ * FROM Input;

INSERT INTO Output3 WITH TRUNCATE SELECT /*+ distinct(key subkey) unique(value) */ * FROM Input;
INSERT INTO Output4 WITH TRUNCATE SELECT /*+ unique(key) distinct(subkey value) */ * FROM Input;
INSERT INTO Output5 WITH TRUNCATE SELECT /*+ unique(key value) unique(subkey) */ * FROM Input;
INSERT INTO Output6 WITH TRUNCATE SELECT /*+ distinct(key) distinct(subkey) */ * FROM Input;

-- Bad case: missed column - ignore hint with warning.
INSERT INTO Output7 WITH TRUNCATE SELECT /*+ unique(subkey value) */ key, value FROM Input;
