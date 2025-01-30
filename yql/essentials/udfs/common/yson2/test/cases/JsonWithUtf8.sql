$src = Yson::From("Хэллоу!");
SELECT Yson::SerializeJson($src, false AS EncodeUtf8), Yson::SerializeJson($src, true AS EncodeUtf8),
    Yson::Equals(Yson::ParseJson(Json("\"\xD0\xA5\xD1\x8D\xD0\xBB\xD0\xBB\xD0\xBE\xD1\x83!\"")), Yson::ParseJsonDecodeUtf8(Json("\"\xC3\x90\xC2\xA5\xC3\x91\xC2\x8D\xC3\x90\xC2\xBB\xC3\x90\xC2\xBB\xC3\x90\xC2\xBE\xC3\x91\xC2\x83!\""))),
    Yson::SerializeJsonEncodeUtf8($src);

