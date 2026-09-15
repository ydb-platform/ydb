use plato;

$a = select CurrentUtcDate() as _date, Just(1.0) as parsed_lag from Input;

SELECT 
    SUM(parsed_lag)
FROM  $a;
