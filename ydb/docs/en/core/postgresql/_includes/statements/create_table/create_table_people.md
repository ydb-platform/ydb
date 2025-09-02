```sql
CREATE TABLE people (
    id                 Serial PRIMARY KEY,
    name               Text,
    lastname           Text,
    age                Int,
    country            Text,
    state              Text,
    city               Text,
    birthday           Date,
    sex                Text,
    social_card_number Int
);
```