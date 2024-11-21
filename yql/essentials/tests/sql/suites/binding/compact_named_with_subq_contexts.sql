/* yt can not */
pragma CompactNamedExprs;

$a = (SELECT CAST(Unicode::ToUpper("o"u) AS String) || "utpu");
$b = $a || CAST(Unicode::ToLower("T"u) AS String);
select $b;
select $a || CAST(Unicode::ToLower("T"u) AS String);
select * from $a;
select "Outpu" in $a;

define subquery $sub() as
  select * from $a;
end define;

select * from $sub();

