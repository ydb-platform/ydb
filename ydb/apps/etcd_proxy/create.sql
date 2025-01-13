CREATE TABLE huidig
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`)
);

CREATE TABLE verleeden
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`, `modified`)
);

