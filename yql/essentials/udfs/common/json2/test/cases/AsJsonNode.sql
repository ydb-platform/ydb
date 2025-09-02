SELECT
    Json2::Utf8AsJsonNode(CAST("string" as Utf8)),
    Json2::Utf8AsJsonNode(NULL),
    Json2::DoubleAsJsonNode(1.2345),
    Json2::DoubleAsJsonNode(NULL),
    Json2::BoolAsJsonNode(true),
    Json2::BoolAsJsonNode(NULL),
    Json2::JsonAsJsonNode(CAST(@@{"key": 28}@@ as Json)),
    Json2::JsonAsJsonNode(NULL);
