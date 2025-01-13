/* postgres can not */
SELECT
    Just(Just(TRUE)) == TRUE,
    Just(FALSE) != Just(Just(FALSE))
;
