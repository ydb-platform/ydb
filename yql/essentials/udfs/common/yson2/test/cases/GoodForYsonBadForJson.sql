$inf = Yson::From(1./0.);
$binary = Yson::From("\"12345\xf67\"");

SELECT Yson::Serialize($inf), Yson::Serialize($binary);

PRAGMA yson.DisableStrict;
SELECT Yson::SerializeJson($inf), Yson::SerializeJson($binary);
