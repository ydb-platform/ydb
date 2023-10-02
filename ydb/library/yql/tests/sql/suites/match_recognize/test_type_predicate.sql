pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

USE plato;

$data = [<||>];

$BadPredicate = select TableRow() from (select * from AS_TABLE($data) MATCH_RECOGNIZE(
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      A
    )
    DEFINE
      A as 123 -- must fail, Bool expected
));

select FormatType(TypeOf($BadPredicate));
