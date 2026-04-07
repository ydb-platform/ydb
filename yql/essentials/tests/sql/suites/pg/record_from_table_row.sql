--!syntax_pg
SELECT row_to_json(TR)
FROM (select 'foo' as y, 1 as x) tr

