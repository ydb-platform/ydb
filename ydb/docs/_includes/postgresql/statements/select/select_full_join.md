```sql
SELECT people.name, people.lastname, card.social_card_number
FROM people 
FULL JOIN social_card AS card
ON people.name = card.card_holder_name AND people.lastname = card.card_holder_lastname;
```