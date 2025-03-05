--!syntax_pg
select id, CodeGen_FullTime
       , "CodeGen_FullTime" as Quoted
       , CodeGen_FullTime || 'x'
       , "CodeGen_FullTime" || 'x'
       , (CodeGen_FullTime || 'x')
from plato."InputC" limit 100;
