```sql
CREATE TABLE social_card (
    id                   Serial PRIMARY KEY,
    social_card_number   Int,
    card_holder_name     Text,
    card_holder_lastname Text,
    issue                Date,
    expiry               Date,
    issuing_authority    Text,
    category             Text
);
```