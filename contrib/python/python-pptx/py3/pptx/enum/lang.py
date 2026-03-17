"""Enumerations used for specifying language."""

from __future__ import annotations

from pptx.enum.base import BaseXmlEnum


class MSO_LANGUAGE_ID(BaseXmlEnum):
    """
    Specifies the language identifier.

    Example::

        from pptx.enum.lang import MSO_LANGUAGE_ID

        font.language_id = MSO_LANGUAGE_ID.POLISH

    MS API Name: `MsoLanguageId`

    https://msdn.microsoft.com/en-us/library/office/ff862134.aspx
    """

    NONE = (0, "", "No language specified.")
    """No language specified."""

    AFRIKAANS = (1078, "af-ZA", "The Afrikaans language.")
    """The Afrikaans language."""

    ALBANIAN = (1052, "sq-AL", "The Albanian language.")
    """The Albanian language."""

    AMHARIC = (1118, "am-ET", "The Amharic language.")
    """The Amharic language."""

    ARABIC = (1025, "ar-SA", "The Arabic language.")
    """The Arabic language."""

    ARABIC_ALGERIA = (5121, "ar-DZ", "The Arabic Algeria language.")
    """The Arabic Algeria language."""

    ARABIC_BAHRAIN = (15361, "ar-BH", "The Arabic Bahrain language.")
    """The Arabic Bahrain language."""

    ARABIC_EGYPT = (3073, "ar-EG", "The Arabic Egypt language.")
    """The Arabic Egypt language."""

    ARABIC_IRAQ = (2049, "ar-IQ", "The Arabic Iraq language.")
    """The Arabic Iraq language."""

    ARABIC_JORDAN = (11265, "ar-JO", "The Arabic Jordan language.")
    """The Arabic Jordan language."""

    ARABIC_KUWAIT = (13313, "ar-KW", "The Arabic Kuwait language.")
    """The Arabic Kuwait language."""

    ARABIC_LEBANON = (12289, "ar-LB", "The Arabic Lebanon language.")
    """The Arabic Lebanon language."""

    ARABIC_LIBYA = (4097, "ar-LY", "The Arabic Libya language.")
    """The Arabic Libya language."""

    ARABIC_MOROCCO = (6145, "ar-MA", "The Arabic Morocco language.")
    """The Arabic Morocco language."""

    ARABIC_OMAN = (8193, "ar-OM", "The Arabic Oman language.")
    """The Arabic Oman language."""

    ARABIC_QATAR = (16385, "ar-QA", "The Arabic Qatar language.")
    """The Arabic Qatar language."""

    ARABIC_SYRIA = (10241, "ar-SY", "The Arabic Syria language.")
    """The Arabic Syria language."""

    ARABIC_TUNISIA = (7169, "ar-TN", "The Arabic Tunisia language.")
    """The Arabic Tunisia language."""

    ARABIC_UAE = (14337, "ar-AE", "The Arabic UAE language.")
    """The Arabic UAE language."""

    ARABIC_YEMEN = (9217, "ar-YE", "The Arabic Yemen language.")
    """The Arabic Yemen language."""

    ARMENIAN = (1067, "hy-AM", "The Armenian language.")
    """The Armenian language."""

    ASSAMESE = (1101, "as-IN", "The Assamese language.")
    """The Assamese language."""

    AZERI_CYRILLIC = (2092, "az-AZ", "The Azeri Cyrillic language.")
    """The Azeri Cyrillic language."""

    AZERI_LATIN = (1068, "az-Latn-AZ", "The Azeri Latin language.")
    """The Azeri Latin language."""

    BASQUE = (1069, "eu-ES", "The Basque language.")
    """The Basque language."""

    BELGIAN_DUTCH = (2067, "nl-BE", "The Belgian Dutch language.")
    """The Belgian Dutch language."""

    BELGIAN_FRENCH = (2060, "fr-BE", "The Belgian French language.")
    """The Belgian French language."""

    BENGALI = (1093, "bn-IN", "The Bengali language.")
    """The Bengali language."""

    BOSNIAN = (4122, "hr-BA", "The Bosnian language.")
    """The Bosnian language."""

    BOSNIAN_BOSNIA_HERZEGOVINA_CYRILLIC = (
        8218,
        "bs-BA",
        "The Bosnian Bosnia Herzegovina Cyrillic language.",
    )
    """The Bosnian Bosnia Herzegovina Cyrillic language."""

    BOSNIAN_BOSNIA_HERZEGOVINA_LATIN = (
        5146,
        "bs-Latn-BA",
        "The Bosnian Bosnia Herzegovina Latin language.",
    )
    """The Bosnian Bosnia Herzegovina Latin language."""

    BRAZILIAN_PORTUGUESE = (1046, "pt-BR", "The Brazilian Portuguese language.")
    """The Brazilian Portuguese language."""

    BULGARIAN = (1026, "bg-BG", "The Bulgarian language.")
    """The Bulgarian language."""

    BURMESE = (1109, "my-MM", "The Burmese language.")
    """The Burmese language."""

    BYELORUSSIAN = (1059, "be-BY", "The Byelorussian language.")
    """The Byelorussian language."""

    CATALAN = (1027, "ca-ES", "The Catalan language.")
    """The Catalan language."""

    CHEROKEE = (1116, "chr-US", "The Cherokee language.")
    """The Cherokee language."""

    CHINESE_HONG_KONG_SAR = (3076, "zh-HK", "The Chinese Hong Kong SAR language.")
    """The Chinese Hong Kong SAR language."""

    CHINESE_MACAO_SAR = (5124, "zh-MO", "The Chinese Macao SAR language.")
    """The Chinese Macao SAR language."""

    CHINESE_SINGAPORE = (4100, "zh-SG", "The Chinese Singapore language.")
    """The Chinese Singapore language."""

    CROATIAN = (1050, "hr-HR", "The Croatian language.")
    """The Croatian language."""

    CZECH = (1029, "cs-CZ", "The Czech language.")
    """The Czech language."""

    DANISH = (1030, "da-DK", "The Danish language.")
    """The Danish language."""

    DIVEHI = (1125, "div-MV", "The Divehi language.")
    """The Divehi language."""

    DUTCH = (1043, "nl-NL", "The Dutch language.")
    """The Dutch language."""

    EDO = (1126, "bin-NG", "The Edo language.")
    """The Edo language."""

    ENGLISH_AUS = (3081, "en-AU", "The English AUS language.")
    """The English AUS language."""

    ENGLISH_BELIZE = (10249, "en-BZ", "The English Belize language.")
    """The English Belize language."""

    ENGLISH_CANADIAN = (4105, "en-CA", "The English Canadian language.")
    """The English Canadian language."""

    ENGLISH_CARIBBEAN = (9225, "en-CB", "The English Caribbean language.")
    """The English Caribbean language."""

    ENGLISH_INDONESIA = (14345, "en-ID", "The English Indonesia language.")
    """The English Indonesia language."""

    ENGLISH_IRELAND = (6153, "en-IE", "The English Ireland language.")
    """The English Ireland language."""

    ENGLISH_JAMAICA = (8201, "en-JA", "The English Jamaica language.")
    """The English Jamaica language."""

    ENGLISH_NEW_ZEALAND = (5129, "en-NZ", "The English NewZealand language.")
    """The English NewZealand language."""

    ENGLISH_PHILIPPINES = (13321, "en-PH", "The English Philippines language.")
    """The English Philippines language."""

    ENGLISH_SOUTH_AFRICA = (7177, "en-ZA", "The English South Africa language.")
    """The English South Africa language."""

    ENGLISH_TRINIDAD_TOBAGO = (11273, "en-TT", "The English Trinidad Tobago language.")
    """The English Trinidad Tobago language."""

    ENGLISH_UK = (2057, "en-GB", "The English UK language.")
    """The English UK language."""

    ENGLISH_US = (1033, "en-US", "The English US language.")
    """The English US language."""

    ENGLISH_ZIMBABWE = (12297, "en-ZW", "The English Zimbabwe language.")
    """The English Zimbabwe language."""

    ESTONIAN = (1061, "et-EE", "The Estonian language.")
    """The Estonian language."""

    FAEROESE = (1080, "fo-FO", "The Faeroese language.")
    """The Faeroese language."""

    FARSI = (1065, "fa-IR", "The Farsi language.")
    """The Farsi language."""

    FILIPINO = (1124, "fil-PH", "The Filipino language.")
    """The Filipino language."""

    FINNISH = (1035, "fi-FI", "The Finnish language.")
    """The Finnish language."""

    FRANCH_CONGO_DRC = (9228, "fr-CD", "The French Congo DRC language.")
    """The French Congo DRC language."""

    FRENCH = (1036, "fr-FR", "The French language.")
    """The French language."""

    FRENCH_CAMEROON = (11276, "fr-CM", "The French Cameroon language.")
    """The French Cameroon language."""

    FRENCH_CANADIAN = (3084, "fr-CA", "The French Canadian language.")
    """The French Canadian language."""

    FRENCH_COTED_IVOIRE = (12300, "fr-CI", "The French Coted Ivoire language.")
    """The French Coted Ivoire language."""

    FRENCH_HAITI = (15372, "fr-HT", "The French Haiti language.")
    """The French Haiti language."""

    FRENCH_LUXEMBOURG = (5132, "fr-LU", "The French Luxembourg language.")
    """The French Luxembourg language."""

    FRENCH_MALI = (13324, "fr-ML", "The French Mali language.")
    """The French Mali language."""

    FRENCH_MONACO = (6156, "fr-MC", "The French Monaco language.")
    """The French Monaco language."""

    FRENCH_MOROCCO = (14348, "fr-MA", "The French Morocco language.")
    """The French Morocco language."""

    FRENCH_REUNION = (8204, "fr-RE", "The French Reunion language.")
    """The French Reunion language."""

    FRENCH_SENEGAL = (10252, "fr-SN", "The French Senegal language.")
    """The French Senegal language."""

    FRENCH_WEST_INDIES = (7180, "fr-WINDIES", "The French West Indies language.")
    """The French West Indies language."""

    FRISIAN_NETHERLANDS = (1122, "fy-NL", "The Frisian Netherlands language.")
    """The Frisian Netherlands language."""

    FULFULDE = (1127, "ff-NG", "The Fulfulde language.")
    """The Fulfulde language."""

    GAELIC_IRELAND = (2108, "ga-IE", "The Gaelic Ireland language.")
    """The Gaelic Ireland language."""

    GAELIC_SCOTLAND = (1084, "en-US", "The Gaelic Scotland language.")
    """The Gaelic Scotland language."""

    GALICIAN = (1110, "gl-ES", "The Galician language.")
    """The Galician language."""

    GEORGIAN = (1079, "ka-GE", "The Georgian language.")
    """The Georgian language."""

    GERMAN = (1031, "de-DE", "The German language.")
    """The German language."""

    GERMAN_AUSTRIA = (3079, "de-AT", "The German Austria language.")
    """The German Austria language."""

    GERMAN_LIECHTENSTEIN = (5127, "de-LI", "The German Liechtenstein language.")
    """The German Liechtenstein language."""

    GERMAN_LUXEMBOURG = (4103, "de-LU", "The German Luxembourg language.")
    """The German Luxembourg language."""

    GREEK = (1032, "el-GR", "The Greek language.")
    """The Greek language."""

    GUARANI = (1140, "gn-PY", "The Guarani language.")
    """The Guarani language."""

    GUJARATI = (1095, "gu-IN", "The Gujarati language.")
    """The Gujarati language."""

    HAUSA = (1128, "ha-NG", "The Hausa language.")
    """The Hausa language."""

    HAWAIIAN = (1141, "haw-US", "The Hawaiian language.")
    """The Hawaiian language."""

    HEBREW = (1037, "he-IL", "The Hebrew language.")
    """The Hebrew language."""

    HINDI = (1081, "hi-IN", "The Hindi language.")
    """The Hindi language."""

    HUNGARIAN = (1038, "hu-HU", "The Hungarian language.")
    """The Hungarian language."""

    IBIBIO = (1129, "ibb-NG", "The Ibibio language.")
    """The Ibibio language."""

    ICELANDIC = (1039, "is-IS", "The Icelandic language.")
    """The Icelandic language."""

    IGBO = (1136, "ig-NG", "The Igbo language.")
    """The Igbo language."""

    INDONESIAN = (1057, "id-ID", "The Indonesian language.")
    """The Indonesian language."""

    INUKTITUT = (1117, "iu-Cans-CA", "The Inuktitut language.")
    """The Inuktitut language."""

    ITALIAN = (1040, "it-IT", "The Italian language.")
    """The Italian language."""

    JAPANESE = (1041, "ja-JP", "The Japanese language.")
    """The Japanese language."""

    KANNADA = (1099, "kn-IN", "The Kannada language.")
    """The Kannada language."""

    KANURI = (1137, "kr-NG", "The Kanuri language.")
    """The Kanuri language."""

    KASHMIRI = (1120, "ks-Arab", "The Kashmiri language.")
    """The Kashmiri language."""

    KASHMIRI_DEVANAGARI = (2144, "ks-Deva", "The Kashmiri Devanagari language.")
    """The Kashmiri Devanagari language."""

    KAZAKH = (1087, "kk-KZ", "The Kazakh language.")
    """The Kazakh language."""

    KHMER = (1107, "kh-KH", "The Khmer language.")
    """The Khmer language."""

    KIRGHIZ = (1088, "ky-KG", "The Kirghiz language.")
    """The Kirghiz language."""

    KONKANI = (1111, "kok-IN", "The Konkani language.")
    """The Konkani language."""

    KOREAN = (1042, "ko-KR", "The Korean language.")
    """The Korean language."""

    KYRGYZ = (1088, "ky-KG", "The Kyrgyz language.")
    """The Kyrgyz language."""

    LAO = (1108, "lo-LA", "The Lao language.")
    """The Lao language."""

    LATIN = (1142, "la-Latn", "The Latin language.")
    """The Latin language."""

    LATVIAN = (1062, "lv-LV", "The Latvian language.")
    """The Latvian language."""

    LITHUANIAN = (1063, "lt-LT", "The Lithuanian language.")
    """The Lithuanian language."""

    MACEDONINAN_FYROM = (1071, "mk-MK", "The Macedonian FYROM language.")
    """The Macedonian FYROM language."""

    MALAY_BRUNEI_DARUSSALAM = (2110, "ms-BN", "The Malay Brunei Darussalam language.")
    """The Malay Brunei Darussalam language."""

    MALAYALAM = (1100, "ml-IN", "The Malayalam language.")
    """The Malayalam language."""

    MALAYSIAN = (1086, "ms-MY", "The Malaysian language.")
    """The Malaysian language."""

    MALTESE = (1082, "mt-MT", "The Maltese language.")
    """The Maltese language."""

    MANIPURI = (1112, "mni-IN", "The Manipuri language.")
    """The Manipuri language."""

    MAORI = (1153, "mi-NZ", "The Maori language.")
    """The Maori language."""

    MARATHI = (1102, "mr-IN", "The Marathi language.")
    """The Marathi language."""

    MEXICAN_SPANISH = (2058, "es-MX", "The Mexican Spanish language.")
    """The Mexican Spanish language."""

    MONGOLIAN = (1104, "mn-MN", "The Mongolian language.")
    """The Mongolian language."""

    NEPALI = (1121, "ne-NP", "The Nepali language.")
    """The Nepali language."""

    NO_PROOFING = (1024, "en-US", "No proofing.")
    """No proofing."""

    NORWEGIAN_BOKMOL = (1044, "nb-NO", "The Norwegian Bokmol language.")
    """The Norwegian Bokmol language."""

    NORWEGIAN_NYNORSK = (2068, "nn-NO", "The Norwegian Nynorsk language.")
    """The Norwegian Nynorsk language."""

    ORIYA = (1096, "or-IN", "The Oriya language.")
    """The Oriya language."""

    OROMO = (1138, "om-Ethi-ET", "The Oromo language.")
    """The Oromo language."""

    PASHTO = (1123, "ps-AF", "The Pashto language.")
    """The Pashto language."""

    POLISH = (1045, "pl-PL", "The Polish language.")
    """The Polish language."""

    PORTUGUESE = (2070, "pt-PT", "The Portuguese language.")
    """The Portuguese language."""

    PUNJABI = (1094, "pa-IN", "The Punjabi language.")
    """The Punjabi language."""

    QUECHUA_BOLIVIA = (1131, "quz-BO", "The Quechua Bolivia language.")
    """The Quechua Bolivia language."""

    QUECHUA_ECUADOR = (2155, "quz-EC", "The Quechua Ecuador language.")
    """The Quechua Ecuador language."""

    QUECHUA_PERU = (3179, "quz-PE", "The Quechua Peru language.")
    """The Quechua Peru language."""

    RHAETO_ROMANIC = (1047, "rm-CH", "The Rhaeto Romanic language.")
    """The Rhaeto Romanic language."""

    ROMANIAN = (1048, "ro-RO", "The Romanian language.")
    """The Romanian language."""

    ROMANIAN_MOLDOVA = (2072, "ro-MO", "The Romanian Moldova language.")
    """The Romanian Moldova language."""

    RUSSIAN = (1049, "ru-RU", "The Russian language.")
    """The Russian language."""

    RUSSIAN_MOLDOVA = (2073, "ru-MO", "The Russian Moldova language.")
    """The Russian Moldova language."""

    SAMI_LAPPISH = (1083, "se-NO", "The Sami Lappish language.")
    """The Sami Lappish language."""

    SANSKRIT = (1103, "sa-IN", "The Sanskrit language.")
    """The Sanskrit language."""

    SEPEDI = (1132, "ns-ZA", "The Sepedi language.")
    """The Sepedi language."""

    SERBIAN_BOSNIA_HERZEGOVINA_CYRILLIC = (
        7194,
        "sr-BA",
        "The Serbian Bosnia Herzegovina Cyrillic language.",
    )
    """The Serbian Bosnia Herzegovina Cyrillic language."""

    SERBIAN_BOSNIA_HERZEGOVINA_LATIN = (
        6170,
        "sr-Latn-BA",
        "The Serbian Bosnia Herzegovina Latin language.",
    )
    """The Serbian Bosnia Herzegovina Latin language."""

    SERBIAN_CYRILLIC = (3098, "sr-SP", "The Serbian Cyrillic language.")
    """The Serbian Cyrillic language."""

    SERBIAN_LATIN = (2074, "sr-Latn-CS", "The Serbian Latin language.")
    """The Serbian Latin language."""

    SESOTHO = (1072, "st-ZA", "The Sesotho language.")
    """The Sesotho language."""

    SIMPLIFIED_CHINESE = (2052, "zh-CN", "The Simplified Chinese language.")
    """The Simplified Chinese language."""

    SINDHI = (1113, "sd-Deva-IN", "The Sindhi language.")
    """The Sindhi language."""

    SINDHI_PAKISTAN = (2137, "sd-Arab-PK", "The Sindhi Pakistan language.")
    """The Sindhi Pakistan language."""

    SINHALESE = (1115, "si-LK", "The Sinhalese language.")
    """The Sinhalese language."""

    SLOVAK = (1051, "sk-SK", "The Slovak language.")
    """The Slovak language."""

    SLOVENIAN = (1060, "sl-SI", "The Slovenian language.")
    """The Slovenian language."""

    SOMALI = (1143, "so-SO", "The Somali language.")
    """The Somali language."""

    SORBIAN = (1070, "wen-DE", "The Sorbian language.")
    """The Sorbian language."""

    SPANISH = (1034, "es-ES_tradnl", "The Spanish language.")
    """The Spanish language."""

    SPANISH_ARGENTINA = (11274, "es-AR", "The Spanish Argentina language.")
    """The Spanish Argentina language."""

    SPANISH_BOLIVIA = (16394, "es-BO", "The Spanish Bolivia language.")
    """The Spanish Bolivia language."""

    SPANISH_CHILE = (13322, "es-CL", "The Spanish Chile language.")
    """The Spanish Chile language."""

    SPANISH_COLOMBIA = (9226, "es-CO", "The Spanish Colombia language.")
    """The Spanish Colombia language."""

    SPANISH_COSTA_RICA = (5130, "es-CR", "The Spanish Costa Rica language.")
    """The Spanish Costa Rica language."""

    SPANISH_DOMINICAN_REPUBLIC = (7178, "es-DO", "The Spanish Dominican Republic language.")
    """The Spanish Dominican Republic language."""

    SPANISH_ECUADOR = (12298, "es-EC", "The Spanish Ecuador language.")
    """The Spanish Ecuador language."""

    SPANISH_EL_SALVADOR = (17418, "es-SV", "The Spanish El Salvador language.")
    """The Spanish El Salvador language."""

    SPANISH_GUATEMALA = (4106, "es-GT", "The Spanish Guatemala language.")
    """The Spanish Guatemala language."""

    SPANISH_HONDURAS = (18442, "es-HN", "The Spanish Honduras language.")
    """The Spanish Honduras language."""

    SPANISH_MODERN_SORT = (3082, "es-ES", "The Spanish Modern Sort language.")
    """The Spanish Modern Sort language."""

    SPANISH_NICARAGUA = (19466, "es-NI", "The Spanish Nicaragua language.")
    """The Spanish Nicaragua language."""

    SPANISH_PANAMA = (6154, "es-PA", "The Spanish Panama language.")
    """The Spanish Panama language."""

    SPANISH_PARAGUAY = (15370, "es-PY", "The Spanish Paraguay language.")
    """The Spanish Paraguay language."""

    SPANISH_PERU = (10250, "es-PE", "The Spanish Peru language.")
    """The Spanish Peru language."""

    SPANISH_PUERTO_RICO = (20490, "es-PR", "The Spanish Puerto Rico language.")
    """The Spanish Puerto Rico language."""

    SPANISH_URUGUAY = (14346, "es-UR", "The Spanish Uruguay language.")
    """The Spanish Uruguay language."""

    SPANISH_VENEZUELA = (8202, "es-VE", "The Spanish Venezuela language.")
    """The Spanish Venezuela language."""

    SUTU = (1072, "st-ZA", "The Sutu language.")
    """The Sutu language."""

    SWAHILI = (1089, "sw-KE", "The Swahili language.")
    """The Swahili language."""

    SWEDISH = (1053, "sv-SE", "The Swedish language.")
    """The Swedish language."""

    SWEDISH_FINLAND = (2077, "sv-FI", "The Swedish Finland language.")
    """The Swedish Finland language."""

    SWISS_FRENCH = (4108, "fr-CH", "The Swiss French language.")
    """The Swiss French language."""

    SWISS_GERMAN = (2055, "de-CH", "The Swiss German language.")
    """The Swiss German language."""

    SWISS_ITALIAN = (2064, "it-CH", "The Swiss Italian language.")
    """The Swiss Italian language."""

    SYRIAC = (1114, "syr-SY", "The Syriac language.")
    """The Syriac language."""

    TAJIK = (1064, "tg-TJ", "The Tajik language.")
    """The Tajik language."""

    TAMAZIGHT = (1119, "tzm-Arab-MA", "The Tamazight language.")
    """The Tamazight language."""

    TAMAZIGHT_LATIN = (2143, "tmz-DZ", "The Tamazight Latin language.")
    """The Tamazight Latin language."""

    TAMIL = (1097, "ta-IN", "The Tamil language.")
    """The Tamil language."""

    TATAR = (1092, "tt-RU", "The Tatar language.")
    """The Tatar language."""

    TELUGU = (1098, "te-IN", "The Telugu language.")
    """The Telugu language."""

    THAI = (1054, "th-TH", "The Thai language.")
    """The Thai language."""

    TIBETAN = (1105, "bo-CN", "The Tibetan language.")
    """The Tibetan language."""

    TIGRIGNA_ERITREA = (2163, "ti-ER", "The Tigrigna Eritrea language.")
    """The Tigrigna Eritrea language."""

    TIGRIGNA_ETHIOPIC = (1139, "ti-ET", "The Tigrigna Ethiopic language.")
    """The Tigrigna Ethiopic language."""

    TRADITIONAL_CHINESE = (1028, "zh-TW", "The Traditional Chinese language.")
    """The Traditional Chinese language."""

    TSONGA = (1073, "ts-ZA", "The Tsonga language.")
    """The Tsonga language."""

    TSWANA = (1074, "tn-ZA", "The Tswana language.")
    """The Tswana language."""

    TURKISH = (1055, "tr-TR", "The Turkish language.")
    """The Turkish language."""

    TURKMEN = (1090, "tk-TM", "The Turkmen language.")
    """The Turkmen language."""

    UKRAINIAN = (1058, "uk-UA", "The Ukrainian language.")
    """The Ukrainian language."""

    URDU = (1056, "ur-PK", "The Urdu language.")
    """The Urdu language."""

    UZBEK_CYRILLIC = (2115, "uz-UZ", "The Uzbek Cyrillic language.")
    """The Uzbek Cyrillic language."""

    UZBEK_LATIN = (1091, "uz-Latn-UZ", "The Uzbek Latin language.")
    """The Uzbek Latin language."""

    VENDA = (1075, "ve-ZA", "The Venda language.")
    """The Venda language."""

    VIETNAMESE = (1066, "vi-VN", "The Vietnamese language.")
    """The Vietnamese language."""

    WELSH = (1106, "cy-GB", "The Welsh language.")
    """The Welsh language."""

    XHOSA = (1076, "xh-ZA", "The Xhosa language.")
    """The Xhosa language."""

    YI = (1144, "ii-CN", "The Yi language.")
    """The Yi language."""

    YIDDISH = (1085, "yi-Hebr", "The Yiddish language.")
    """The Yiddish language."""

    YORUBA = (1130, "yo-NG", "The Yoruba language.")
    """The Yoruba language."""

    ZULU = (1077, "zu-ZA", "The Zulu language.")
    """The Zulu language."""

    MIXED = (-2, "", "More than one language in specified range (read-only).")
    """More than one language in specified range (read-only)."""
