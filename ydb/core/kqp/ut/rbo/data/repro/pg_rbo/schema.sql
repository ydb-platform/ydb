CREATE TABLE `/Root/pg-rbo/_Reference61509` (
    `_IDRRef` String NOT NULL,
    `_Description` String,
    `_Fld61627` String,
    `_Fld61628` String,
    `_Fld61629` String,
    `_Fld61630` String,
    `_Fld61632` String,
    `_Fld61635RRef` String,
    `_Fld61634` Timestamp,
    `_Fld61636` String,
    `_Fld61637RRef` String,
    `_Fld61638RRef` String,
    `_Fld543` Int32 NOT NULL,
    PRIMARY KEY (`_IDRRef`)
);

CREATE TABLE `/Root/pg-rbo/_InfoRg61621` (
    `_key` Uint64 NOT NULL,
    `_Fld61622RRef` String NOT NULL,
    `_Fld543` Int32 NOT NULL,
    `_Fld61623RRef` String,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt19` (
    `_key` Uint64 NOT NULL,
    `_Q_001_F_000RRef` String NOT NULL,
    `_Q_001_F_002RRef` String NOT NULL,
    `_Q_001_F_006` String,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt22` (
    `_key` Uint64 NOT NULL,
    `_Q_001_F_000RRef` String NOT NULL,
    `_Q_001_F_001RRef` String,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt41` (
    `_key` Uint64 NOT NULL,
    `_Q_001_F_000RRef` String NOT NULL,
    `_Q_001_F_001RRef` String,
    `_Q_001_F_002RRef` String,
    `_Q_001_F_003` Int32,
    `_Q_001_F_004RRef` String,
    `_Q_001_F_005` String,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt35` (
    `_key` Uint64 NOT NULL,
    `_Q_001_F_000RRef` String NOT NULL,
    `_Q_001_F_001RRef` String,
    `_Q_001_F_002RRef` String,
    `_Q_001_F_003` Int32,
    `_Q_001_F_004RRef` String,
    `_Q_001_F_005` String,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt29` (
    `_key` Uint64 NOT NULL,
    `_Q_001_F_000RRef` String NOT NULL,
    `_Q_001_F_001RRef` String,
    `_Q_001_F_002RRef` String,
    `_Q_001_F_003` String,
    `_Q_001_F_004RRef` String,
    `_Q_001_F_005` Int32,
    PRIMARY KEY (`_key`)
);

CREATE TABLE `/Root/pg-rbo/tt16` (
    `_key` Uint64 NOT NULL,
    `_Q_000_F_007RRef` String NOT NULL,
    PRIMARY KEY (`_key`)
);
