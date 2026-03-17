# Purpose: constant values
# Created: 10.03.2011
# Copyright (c) 2011-2018, Manfred Moitzi
# License: MIT License

# --- For Unix ---
#  0:    undefined    Rises an error.
#  1:    ascii        $20-$7F,%%c,%%d,%%p Only
#  2:    iso8859_1    Western Europe(Unix)    $20-$FF,%%c,%%d,%%p
#  3:    iso8859_2    Central Europe(Unix)
#  4:    iso8859_3    Eastern Europe(Unix)
#  5:    iso8859_4    Baltic(Unix)
#  6:    iso8859_5    Cyrillic(Unix)
#  7:    iso8859_6    Arabic(Unix)
#  8:    iso8859_7    Greek(Unix)
#  9:    iso8859_8    Hebrew(Unix)
# 10:    iso8859_9    Turkish(Unix)
#
# --- For DOS/Mac ---
# 11:    dos437    DOS USA
# 12:    dos850    DOS Western Europe
# 13:    dos852    DOS Eastern Europe
# 14:    dos855    IBM Russian
# 15:    dos857    IBM Turkish
# 16:    dos860    DOS Portuguese
# 17:    dos861    DOS Icelandic
# 18:    dos863    DOS Canadian French
# 19:    dos864    DOS Arabic
# 20:    dos865    DOS Norwegian
# 21:    dos869    DOS Greek
# 22:    dos932    DOS Japanese
# 23:    mac-roman    Mac
# 24:    big5    DOS Traditional Chinese
# 25:    ksc5601    Korean Wansung
# 26:    johab    Korean Johab
# 27:    dos866    DOS Russian
# 31:    gb2312    DOS Simplified Chinese
#
# --- For Windows ---
# 28:    ansi_1250    Win Eastern Europe
# 29:    ansi_1251    Win Russian
# 30:    ansi_1252    Win Western Europe(ANSI)
# 32:    ansi_1253    Win Greek
# 33:    ansi_1254    Win Turkish
# 34:    ansi_1255    Win Hebrew
# 35:    ansi_1256    Win Arabic
# 36:    ansi_1257    Win Baltic
# 37:    ansi_874    Win Thai
# 38:    ansi_932    Win Japanese
# 39:    ansi_936    Win Simplified Chinese GB
# 40:    ansi_949    Win Korean Wansung
# 41:    ansi_950    Win Traditional Chinese big5
# 42:    ansi_1361    Win Korean Johab
# 43:    ansi_1200    Unicode (reserved)
# --:    ansi_1258    Win Vietnamese (reserved)

codepage_to_encoding = {
    "874": "cp874",  # Thai,
    "932": "cp932",  # Japanese
    "936": "gbk",  # UnifiedChinese
    "949": "cp949",  # Korean
    "950": "cp950",  # TradChinese
    "1250": "cp1250",  # CentralEurope
    "1251": "cp1251",  # Cyrillic
    "1252": "cp1252",  # WesternEurope
    "1253": "cp1253",  # Greek
    "1254": "cp1254",  # Turkish
    "1255": "cp1255",  # Hebrew
    "1256": "cp1256",  # Arabic
    "1257": "cp1257",  # Baltic
    "1258": "cp1258",  # Vietnam
}

encoding_to_codepage = {
    codec: ansi for ansi, codec in codepage_to_encoding.items()
}


def is_supported_encoding(encoding: str = "cp1252") -> bool:
    return encoding in encoding_to_codepage


def toencoding(dxfcodepage: str) -> str:
    for codepage, encoding in codepage_to_encoding.items():
        if dxfcodepage.endswith(codepage):
            return encoding
    return "cp1252"


def tocodepage(encoding: str) -> str:
    return "ANSI_" + encoding_to_codepage.get(encoding, "1252")
