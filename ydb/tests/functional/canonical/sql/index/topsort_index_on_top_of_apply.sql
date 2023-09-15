--!syntax_v1 
SELECT 
String::AsciiToLower(t.Value) as Value_alias, 
t.Index1A as Index1A_alias,
String::AsciiToLower(t.Index1B) as index1b_alias
from NewTableWithIndex view NewIndex1 as t
order by t.Index1A_alias desc, t.index1b_alias desc 
limit 2;
