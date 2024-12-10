PRAGMA yson.Strict;

DECLARE $x AS Yson;

SELECT
    ToBytes(Yson::SerializePretty($x))
;
