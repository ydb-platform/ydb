```sql
CREATE TABLE people (
    id                    Serial,
    name                  Text NOT NULL,
    lastname              Text NOT NULL,
    age                   Int,
    country               Text,
    state                 Text,
    city                  Text,
    birthday              Date,
    sex                   Text NOT NULL,
    social_card_number    Int,
    CONSTRAINT pk PRIMARY KEY(id)
);
```