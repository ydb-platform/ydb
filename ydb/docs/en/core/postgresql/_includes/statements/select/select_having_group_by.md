```sql
SELECT sex, country, age
FROM people
GROUP BY sex, country, age
HAVING sex = 'Female';
```