--!syntax_pg
select min(oid) as minoid, max(oid) as maxoid, 
       min(proname) as minproname, max(proname) as maxproname, 
       min(pronamespace) as minpronamespace, max(pronamespace) as maxpronamespace,
       min(proowner) as minproowner, max(proowner) as maxproowner,
       min(prorettype) as minprorettype, max(prorettype) as maxprorettype,
       min(prolang) as minprolang, max(prolang) as maxprolang,
       min(prokind) as minprokind, max(prokind) as maxprokind from pg_proc;
