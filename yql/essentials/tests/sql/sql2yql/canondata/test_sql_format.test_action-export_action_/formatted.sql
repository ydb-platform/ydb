/* syntax version 1 */
/* postgres can not */
/* kikimr can not - range not supported */
PRAGMA library('lib1.sql');

IMPORT lib1 SYMBOLS $action;

DO
    $action('Input')
;
