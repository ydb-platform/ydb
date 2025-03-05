/* custom error:Member not found*/
$json = CAST('{}' AS Json);

SELECT
    JSON_VALUE ($json, 'strict $.key' ERROR ON ERROR)
;
