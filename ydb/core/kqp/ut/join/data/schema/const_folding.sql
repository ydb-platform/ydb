CREATE TABLE test_table (
    BalanceChangeAmount Int64 NOT NULL,
    BalanceChangeKindSystemName Utf8 NOT NULL,
    BalanceChangeDateTimeUtc Utf8 NOT NULL,
    PRIMARY KEY (BalanceChangeAmount)
);