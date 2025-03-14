USE plato;

INSERT INTO @tmp
WITH TRUNCATE
SELECT * from Input
where key == "075";

COMMIT;

INSERT INTO @res1
WITH TRUNCATE
SELECT *
FROM @tmp;

INSERT INTO @tmp
WITH TRUNCATE
SELECT * from Input
where key == "020";

COMMIT;

INSERT INTO @res2
WITH TRUNCATE
SELECT *
FROM @tmp;

COMMIT;

select * from @res1;
select * from @res2;
