/* syntax version 1 */
/* postgres can not */
SELECT
    String::CollapseText('abc', 1)
;

SELECT
    String::CollapseText(Nothing(String?), 1)
;

SELECT
    String::CollapseText(NULL, 1)
;
