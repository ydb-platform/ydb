```sql
UPDATE people
SET age = CASE
            WHEN name = 'John' THEN 32
            WHEN name = 'Jane' THEN 26
          END
WHERE name IN ('John', 'Jane');
```