-- Execute in the Meta database before enabling meta.healthcheck.
-- This is a current snapshot, not a history of checks.
CREATE TABLE `ydb/DatabaseHealth.db` (
    Cluster Utf8 NOT NULL,
    Database Utf8 NOT NULL,
    Status Utf8,
    CheckedAt Timestamp,
    PRIMARY KEY (Cluster, Database)
) WITH (
    TTL = Interval("P1D") ON CheckedAt
);

-- One lease and latest aggregate SelfCheck status per cluster. Full HC JSON is
-- deliberately not persisted: it lives only in the current owner's memory.
CREATE TABLE `ydb/ClusterHealth.db` (
    Cluster Utf8 NOT NULL,
    Owner Utf8,
    ForwardEndpoint Utf8,
    ServiceEndpoint Utf8,
    LastCheck Timestamp,
    Status Utf8,
    CheckedAt Timestamp,
    PRIMARY KEY (Cluster)
);
