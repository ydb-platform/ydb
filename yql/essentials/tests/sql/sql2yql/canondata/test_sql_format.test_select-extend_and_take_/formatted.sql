/* syntax version 1 */
/* postgres can not */
SELECT
    ListExtend(String::SplitToList('1234 123', ' '), String::SplitToList('1234 123', ' '))[1]
;
