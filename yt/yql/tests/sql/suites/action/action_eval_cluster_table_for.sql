/* syntax version 1 */
/* postgres can not */
define action $a($x) as
    $foo = CAST(Unicode::ToLower($x) AS String);

    insert into yt:$foo.Output
    select *
    from yt:$foo.Input
    where key < "100"
    order by key;

end define;

evaluate for $i in AsList("PLATO"u) do $a($i);
