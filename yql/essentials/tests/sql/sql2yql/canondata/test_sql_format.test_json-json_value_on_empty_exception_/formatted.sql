/* custom error:Result is empty*/
$json = CAST("{}" AS Json);

SELECT
    JSON_VALUE ($json, "lax $.key" ERROR ON EMPTY)
;
