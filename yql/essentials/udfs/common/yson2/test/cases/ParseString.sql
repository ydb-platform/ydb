$options = Yson::Options(false AS Strict);

SELECT
    Yson::Parse("0u"),
    Yson::Parse(Just("1u")),
    Yson::ParseJson("2"),
    Yson::ParseJson(Just("3")),
    Yson::Parse("", $options),
    Yson::Parse(Just(""), $options),
    Yson::ParseJson("", $options),
    Yson::ParseJson(Just(""), $options);
