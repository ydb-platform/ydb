USE plato;

select a,bb from (
select a,count(distinct b) as bb,max(c) as cc,median(c) as cc1,percentile(c,0.8) as cc2 from (
select a,b,cast(c as int32) as c,d from Input
)
group by a
)
order by a