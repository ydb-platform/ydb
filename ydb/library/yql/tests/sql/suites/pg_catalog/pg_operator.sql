--!syntax_pg
select 
    min(oid) as min_oid, 
    min(oprcom) as min_oprcom, 
    min(oprleft) as min_oprleft, 
    min(oprname) as min_oprname, 
    min(oprnamespace) as min_oprnamespace, 
    min(oprnegate) as min_oprnegate, 
    min(oprowner) as min_oprowner, 
    min(oprresult) as min_oprresult,
    min(oprright) as min_oprright,
    
    max(oid) as max_oid, 
    max(oprcom) as max_oprcom, 
    max(oprleft) as max_oprleft, 
    max(oprname) as max_oprname, 
    max(oprnamespace) as max_oprnamespace, 
    max(oprnegate) as max_oprnegate, 
    max(oprowner) as max_oprowner, 
    max(oprresult) as max_oprresult,
    max(oprright) as max_oprright
from pg_operator;

