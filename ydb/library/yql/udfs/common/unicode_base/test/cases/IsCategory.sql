/* syntax version 1 */
SELECT
    Unicode::IsAscii("sdf"u),
    Unicode::IsAscii("выавыа"u),
    Unicode::IsSpace(" \u2002\u200a"u),
    Unicode::IsSpace("выавыа"u),
    Unicode::IsUpper("ФЫВ"u),
    Unicode::IsUpper("вВаВыа"u),
    Unicode::IsLower("фыв"u),
    Unicode::IsLower("вВаВыа"u),
    Unicode::IsDigit("1234"u),
    Unicode::IsDigit("выавыа"u),
    Unicode::IsAlpha("фвфы"u),
    Unicode::IsAlpha("вы2в-а"u),
    Unicode::IsAlnum("фыв13в"u),
    Unicode::IsAlnum("выа1-}ыв"u),
    Unicode::IsHex("0F3A4E"u),
    Unicode::IsHex("ваоао"u),
    Unicode::IsUnicodeSet("ваоао"u, "[вао]"u),
    Unicode::IsUnicodeSet("ваоао"u, "[ваб]"u)

