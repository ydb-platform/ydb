pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

USE plato;

$data = [<||>];

select * from (select * from AS_TABLE($data) MATCH_RECOGNIZE(
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (
      A
    )
    DEFINE
      A as True
));
