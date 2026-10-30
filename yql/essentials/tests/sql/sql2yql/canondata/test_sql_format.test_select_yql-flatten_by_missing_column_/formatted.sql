/* custom error: No such column: missing */
PRAGMA YqlSelect = 'force';

SELECT
    *
FROM
    AsTable([<|items: [1, 2]|>])
    FLATTEN LIST BY (
        ListReverse(missing) AS item
    )
;
