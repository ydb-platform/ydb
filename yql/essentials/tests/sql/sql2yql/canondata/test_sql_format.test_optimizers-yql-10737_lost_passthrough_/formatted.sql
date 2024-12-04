/* postgres can not */
/* multirun can not */
/* syntax version 1 */
/* kikimr can not - table truncate */
USE plato;

INSERT INTO @a
SELECT
    "1" AS Text,
    ["a", "b"] AS Attachments;
COMMIT;

SELECT
    x.*,
    "" AS Text,
    ListCreate(TypeOf(Attachments)) AS Attachments
WITHOUT
    x.Text,
    x.Attachments
FROM @a
    AS x;
