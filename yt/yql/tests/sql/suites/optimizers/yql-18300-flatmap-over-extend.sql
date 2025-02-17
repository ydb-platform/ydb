USE plato;

insert into @tmp with truncate
select "dummy" as a, "1" as b, ["b", "s"] as data
order by a;

commit;


SELECT a, id
FROM (
    SELECT
        a,
        ListExtend(
            [String::AsciiToLower(b)],
            ListMap(data, String::AsciiToLower)
        ) AS joins
    FROM @tmp
) FLATTEN LIST BY joins AS id;
