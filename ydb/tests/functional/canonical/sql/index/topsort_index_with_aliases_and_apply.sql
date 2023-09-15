--!syntax_v1 
SELECT 
String::AsciiToLower(t.Value) as Value_alias, 
t.Index1A as Index1A_alias,
t.Index1B
from NewTableWithIndex view NewIndex1 as t
order by t.Index1A_alias desc, t.Index1B desc 
limit 2;
