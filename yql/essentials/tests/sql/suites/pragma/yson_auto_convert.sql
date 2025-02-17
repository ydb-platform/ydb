/* postgres can not */
PRAGMA yson.AutoConvert;
PRAGMA yson.Strict;

SELECT Yson::ConvertToString(Yson("[123;]").0);
