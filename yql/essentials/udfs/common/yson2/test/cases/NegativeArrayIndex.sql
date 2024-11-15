$node = Yson::Parse(@@[1;2;3;4;5;6;7]@@);

SELECT
    Yson::YPathInt64($node, "/+1"),
    Yson::YPathInt64($node, "/-1"),
    Yson::YPathInt64($node, "/+2"),
    Yson::YPathInt64($node, "/-2"),
    Yson::YPathInt64($node, "/+6"),
    Yson::YPathInt64($node, "/-7"),
    Yson::YPathInt64($node, "/+7"),
    Yson::YPathInt64($node, "/-8"),
    Yson::YPathInt64($node, "/0");

