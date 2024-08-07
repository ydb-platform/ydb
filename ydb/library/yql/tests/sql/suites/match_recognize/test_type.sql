pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");
USE plato;

$data = [<|dt:4, host:"fqdn1", key:14|>];


-- NoPartitionNoMeasure
select * from AS_TABLE($data) MATCH_RECOGNIZE(
    ORDER BY CAST(dt as Timestamp)
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      Y
    )
    DEFINE
      Y as NULL
);

--NoPartitionStringMeasure
select * from AS_TABLE($data) MATCH_RECOGNIZE(
    ORDER BY CAST(dt as Timestamp)
    MEASURES
      "SomeString" as Measure1
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      Q
    )
    DEFINE
      Q as TRUE
);

--IntPartitionColNoMeasure
select * from AS_TABLE($data) MATCH_RECOGNIZE(
    PARTITION BY dt
    ORDER BY CAST(dt as Timestamp)
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      L
    )
    DEFINE
      L as JUST(TRUE)
);

--StringPartitionColStringMeasure
select * from AS_TABLE($data) MATCH_RECOGNIZE(
    PARTITION BY host
    ORDER BY CAST(dt as Timestamp)
    MEASURES
      "SomeString" as Measure1
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      Y
    )
    DEFINE
      Y as TRUE
);

--TwoPartitionColsTwoMeasures
select * from AS_TABLE($data) MATCH_RECOGNIZE(
    PARTITION BY host, dt
    ORDER BY CAST(dt as Timestamp)
    MEASURES
      "SomeString" as S,
      345 as I
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      Q
    )
    DEFINE
      Q as JUST(TRUE)
);
