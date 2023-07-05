--!syntax_v1
CREATE TABLE `%s` (
    Key Int32,
    Index1 Int32,
    Value String,
    PRIMARY KEY(Key),
    INDEX NewIndex1 GLOBAL ON (Index1)
);
