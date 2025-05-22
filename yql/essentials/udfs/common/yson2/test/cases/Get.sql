$list = Yson::Parse("[\"abc\"; 123; #;]");
$dict = Yson::Parse("{\"a\"=1;}");
$scalar = Yson::Parse("123");

SELECT
    Yson::GetLength($list) AS list_length,
    Yson::GetLength($dict) AS dict_length,
    Yson::GetLength($scalar, Yson::Options(false AS Strict)) AS scalar_length;
