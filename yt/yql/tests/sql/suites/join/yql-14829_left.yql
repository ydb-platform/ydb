USE plato;

pragma yt.JoinEnableStarJoin="true";

insert into @x
select Just('xxx') as id
order by id;

insert into @y
select Just('yyy') as id
order by id;

insert into @a
select Just('aaa') as id
order by id;

commit;


$xy_left = (
    SELECT 
        x.id AS id 
    FROM 
        ANY @x AS x
        LEFT JOIN ANY @y AS y
        ON x.id == y.id
);

SELECT
    *
FROM 
    ANY @a AS a
    LEFT JOIN ANY $xy_left AS b
    ON a.id == b.id;

