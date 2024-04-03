```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people 
CROSS JOIN social_card AS card
LIMIT 5;
```