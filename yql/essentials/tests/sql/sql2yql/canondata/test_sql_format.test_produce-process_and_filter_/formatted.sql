/* postgres can not */
PROCESS pLaTo.Input0
USING SimpleUdf::Echo(value) AS val
WHERE
    value == "abc"
;
