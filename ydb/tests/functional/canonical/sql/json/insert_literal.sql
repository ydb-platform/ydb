--!syntax_v1

INSERT INTO ResultLiteralJD (Key, Value)
VALUES
    (4, JsonDocument(@@{"name": "George", "age": 23}@@)),
    (5, JsonDocument(@@{"name": "Alex", "age": 65}@@));
