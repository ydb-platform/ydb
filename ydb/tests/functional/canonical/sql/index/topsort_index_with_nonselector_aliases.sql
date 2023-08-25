--!syntax_v1 
SELECT 
t.Value as Value_alias, 
t.Index1A,
t.Index1B
from NewTableWithIndex view NewIndex1 as t
order by t.Index1A desc, t.Index1B desc 
limit 2;
