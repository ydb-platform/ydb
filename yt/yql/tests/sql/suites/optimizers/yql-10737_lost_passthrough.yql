/* postgres can not */
/* multirun can not */
/* syntax version 1 */
/* kikimr can not - table truncate */
USE plato;

insert into @a
select "1" as Text, ["a", "b"] as Attachments;

commit;

SELECT x.*, "" AS Text, ListCreate(TypeOf(Attachments)) AS Attachments
WITHOUT x.Text, x.Attachments
FROM @a AS x
;