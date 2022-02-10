--!syntax_v1

INSERT INTO ResultLiteralDyNumber (Key, Value)
VALUES
    (DyNumber("1.23"), Json(@@{"name": "George", "age": 23}@@)),
    (DyNumber("4.56"), Json(@@{"name": "Alex", "age": 65}@@));
