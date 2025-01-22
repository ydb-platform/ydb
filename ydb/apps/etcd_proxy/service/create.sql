CREATE TABLE verhaal
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`, `modified`)
);

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

ALTER TABLE huidig ADD CHANGEFEED zonder_voorafgaande WITH (format="JSON", mode="UPDATES");
ALTER TABLE huidig ADD CHANGEFEED met_voorafgaande WITH (format="JSON", mode="OLD_IMAGE");
