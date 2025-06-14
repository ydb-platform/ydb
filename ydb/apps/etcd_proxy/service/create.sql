CREATE TABLE current
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`),
    INDEX `lease` GLOBAL ON (`lease`)

)
WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 23, AUTO_PARTITIONING_PARTITION_SIZE_MB = 11);

CREATE TABLE history
(
    `key` Bytes NOT NULL,
    `created` Int64 NOT NULL,
    `modified` Int64 NOT NULL,
    `version` Int64 NOT NULL,
    `value` Bytes NOT NULL,
    `lease` Int64 NOT NULL,
    PRIMARY KEY (`key`, `modified`)
)
WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 23, AUTO_PARTITIONING_PARTITION_SIZE_MB = 101);

CREATE TABLE leases
(
    `id` Int64 NOT NULL,
    `ttl` Int64 NOT NULL,
    `created` Datetime NOT NULL,
    `updated` Datetime NOT NULL,
    PRIMARY KEY (`id`)
)
WITH (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 23, AUTO_PARTITIONING_PARTITION_SIZE_MB = 11);

ALTER TABLE history ADD CHANGEFEED changes WITH (format="JSON", mode="UPDATES");
