```sql
-- Start of the transaction
BEGIN;

-- Data update instruction
UPDATE people
SET name = 'Ivan'
WHERE id = 1;

-- Another data update instruction
UPDATE people
SET lastname = 'Gray'
WHERE id = 10;

-- If all the data is correct, then you need to execute the transaction confirmation instruction:
COMMIT;
```