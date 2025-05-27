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


$xy_leftonly = (
    SELECT 
        x.id AS id 
    FROM 
        @x AS x
        LEFT ONLY JOIN @y AS y
        ON x.id == y.id
);

SELECT
    *
FROM 
    @a AS a
    LEFT ONLY JOIN $xy_leftonly AS b
    ON a.id == b.id;

