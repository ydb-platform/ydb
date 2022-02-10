select sum(A) as B from ( select 5+14 as A union all select -count(*) as A from (select -1 as Y));

