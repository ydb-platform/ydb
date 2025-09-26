use plato;
pragma yt.JoinMergeTablesLimit="10";

$i = select a.key as key, a.subkey as subkey, b.value as value
    from Input as a
    inner join Input as b
    using(key, subkey)
assume order by key,subkey;

insert into Output
select AsTuple(key, subkey) as k, value || "a" as v from $i
assume order by k;
