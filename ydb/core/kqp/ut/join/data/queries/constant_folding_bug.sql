$to_date = ($dt) -> (
    DateTime::Parse("%Y-%m-%d %H:%M")($dt)
);

$in = SELECT $to_date(BalanceChangeDateTimeUtc) as A1, BalanceChangeKindSystemName as A2  FROM `/Root/test_table`;

SELECT *  FROM $in
where A2 in ("Custom");