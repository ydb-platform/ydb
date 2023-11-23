```sql
CREATE TABLE people (
    id                   Serial PRIMARY KEY,
    name                 Text COLLATE "en_US",
    lastname             Text COLLATE "en_US",
    age                  Int,
    country              Text,
    state                Text,
    city                 Text,
    birthday             Date,
    sex                  Text,
    social_card_number   Int
);
```