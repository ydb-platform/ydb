PRAGMA DisableSimpleColumns;
/* postgres can not */
$input = (
select 2 as id,
3 as taskId,
4 as previousId
union all

select 1 as id,
null as taskId,
2 as previousId
);

SELECT count(*)
FROM $input AS diff
INNER JOIN $input AS taskSuite ON diff.previousId = taskSuite.id
LEFT JOIN $input AS pedestrian ON diff.taskId = pedestrian.id
WHERE
diff.id = 1
;

