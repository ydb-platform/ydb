```sql
SELECT <table name left>.<column name>, ... , 
FROM <table name left> 
LEFT | RIGHT | INNER | FULL JOIN <table name right> AS <table name right alias>
ON <table name left>.<column name> = <table name right>.<column name>;
```    