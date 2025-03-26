CREATE TABLE content
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`, `modified`)
);

CREATE TABLE leases
(
    `id` Int64 NOT NULL,
    `ttl` Int64 NOT NULL,
    `created` Datetime NOT NULL,
    `updated` Datetime NOT NULL,
    PRIMARY KEY (`id`)
);

ALTER TABLE content ADD CHANGEFEED changes WITH (format="JSON", mode="UPDATES");
