/* syntax version 1 */
/* postgres can not */
/* kikimr can not - range not supported */
pragma library("lib1.sql");
import lib1 symbols $action;
do $action("Input");
