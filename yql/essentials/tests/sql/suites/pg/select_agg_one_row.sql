--!syntax_pg
select 
    count(*) c1,count(0) c2,count(null) c3,count(null::text) c4,
    min(5) i1,min('a') i2,min(1.0) i3,min(null) i4,min(null::text) i5,
    max(6) a1,max('a') a2,max(1.0) a3,max(null) a4,max(null::text) a5,
    sum(7) s1,sum(1.0) s2,sum(null::int) s3
