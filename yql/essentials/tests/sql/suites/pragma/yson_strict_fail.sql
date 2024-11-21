/* postgres can not */
PRAGMA yson.Strict;

SELECT Yson::ConvertToString(Yson("122"));
