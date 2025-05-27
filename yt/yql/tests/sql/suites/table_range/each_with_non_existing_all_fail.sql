/* syntax version 1 */
/* kikimr can not - range not supported */
/* custom error:The list of tables is empty*/
USE plato;
SELECT * FROM each(["Input1", "Input2", "Input3"]);
