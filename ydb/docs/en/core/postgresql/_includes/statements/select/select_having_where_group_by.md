```sql
SELECT sex, name,age
FROM people
WHERE age > 40
GROUP BY sex, name, age
HAVING sex = 'Female';
```