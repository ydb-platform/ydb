# encoding: utf-8

"""
Enumerations used for specifying language.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

from .base import (
    EnumMember,
    ReturnValueOnlyEnumMember,
    XmlEnumeration,
    XmlMappedEnumMember,
)


class MSO_LANGUAGE_ID(XmlEnumeration):
    """
    Specifies the language identifier.

    Example::

        from pptx.enum.lang import MSO_LANGUAGE_ID

        font.language_id = MSO_LANGUAGE_ID.POLISH
    """

    __ms_name__ = "MsoLanguageId"

    __url__ = "https://msdn.microsoft.com/en-us/library/office/ff862134.aspx"

    __members__ = (
        ReturnValueOnlyEnumMember(
            "MIXED", -2, "More than one language in specified range."
        ),
        EnumMember("NONE", 0, "No language specified."),
        XmlMappedEnumMember("AFRIKAANS", 1078, "af-ZA", "The Afrikaans language."),
        XmlMappedEnumMember("ALBANIAN", 1052, "sq-AL", "The Albanian language."),
        XmlMappedEnumMember("AMHARIC", 1118, "am-ET", "The Amharic language."),
        XmlMappedEnumMember("ARABIC", 1025, "ar-SA", "The Arabic language."),
        XmlMappedEnumMember(
            "ARABIC_ALGERIA", 5121, "ar-DZ", "The Arabic Algeria language."
        ),
        XmlMappedEnumMember(
            "ARABIC_BAHRAIN", 15361, "ar-BH", "The Arabic Bahrain language."
        ),
        XmlMappedEnumMember(
            "ARABIC_EGYPT", 3073, "ar-EG", "The Arabic Egypt language."
        ),
        XmlMappedEnumMember("ARABIC_IRAQ", 2049, "ar-IQ", "The Arabic Iraq language."),
        XmlMappedEnumMember(
            "ARABIC_JORDAN", 11265, "ar-JO", "The Arabic Jordan language."
        ),
        XmlMappedEnumMember(
            "ARABIC_KUWAIT", 13313, "ar-KW", "The Arabic Kuwait language."
        ),
        XmlMappedEnumMember(
            "ARABIC_LEBANON", 12289, "ar-LB", "The Arabic Lebanon language."
        ),
        XmlMappedEnumMember(
            "ARABIC_LIBYA", 4097, "ar-LY", "The Arabic Libya language."
        ),
        XmlMappedEnumMember(
            "ARABIC_MOROCCO", 6145, "ar-MA", "The Arabic Morocco language."
        ),
        XmlMappedEnumMember("ARABIC_OMAN", 8193, "ar-OM", "The Arabic Oman language."),
        XmlMappedEnumMember(
            "ARABIC_QATAR", 16385, "ar-QA", "The Arabic Qatar language."
        ),
        XmlMappedEnumMember(
            "ARABIC_SYRIA", 10241, "ar-SY", "The Arabic Syria language."
        ),
        XmlMappedEnumMember(
            "ARABIC_TUNISIA", 7169, "ar-TN", "The Arabic Tunisia language."
        ),
        XmlMappedEnumMember("ARABIC_UAE", 14337, "ar-AE", "The Arabic UAE language."),
        XmlMappedEnumMember(
            "ARABIC_YEMEN", 9217, "ar-YE", "The Arabic Yemen language."
        ),
        XmlMappedEnumMember("ARMENIAN", 1067, "hy-AM", "The Armenian language."),
        XmlMappedEnumMember("ASSAMESE", 1101, "as-IN", "The Assamese language."),
        XmlMappedEnumMember(
            "AZERI_CYRILLIC", 2092, "az-AZ", "The Azeri Cyrillic language."
        ),
        XmlMappedEnumMember(
            "AZERI_LATIN", 1068, "az-Latn-AZ", "The Azeri Latin language."
        ),
        XmlMappedEnumMember("BASQUE", 1069, "eu-ES", "The Basque language."),
        XmlMappedEnumMember(
            "BELGIAN_DUTCH", 2067, "nl-BE", "The Belgian Dutch language."
        ),
        XmlMappedEnumMember(
            "BELGIAN_FRENCH", 2060, "fr-BE", "The Belgian French language."
        ),
        XmlMappedEnumMember("BENGALI", 1093, "bn-IN", "The Bengali language."),
        XmlMappedEnumMember("BOSNIAN", 4122, "hr-BA", "The Bosnian language."),
        XmlMappedEnumMember(
            "BOSNIAN_BOSNIA_HERZEGOVINA_CYRILLIC",
            8218,
            "bs-BA",
            "The Bosni" "an Bosnia Herzegovina Cyrillic language.",
        ),
        XmlMappedEnumMember(
            "BOSNIAN_BOSNIA_HERZEGOVINA_LATIN",
            5146,
            "bs-Latn-BA",
            "The Bos" "nian Bosnia Herzegovina Latin language.",
        ),
        XmlMappedEnumMember(
            "BRAZILIAN_PORTUGUESE",
            1046,
            "pt-BR",
            "The Brazilian Portuguese" " language.",
        ),
        XmlMappedEnumMember("BULGARIAN", 1026, "bg-BG", "The Bulgarian language."),
        XmlMappedEnumMember("BURMESE", 1109, "my-MM", "The Burmese language."),
        XmlMappedEnumMember(
            "BYELORUSSIAN", 1059, "be-BY", "The Byelorussian language."
        ),
        XmlMappedEnumMember("CATALAN", 1027, "ca-ES", "The Catalan language."),
        XmlMappedEnumMember("CHEROKEE", 1116, "chr-US", "The Cherokee language."),
        XmlMappedEnumMember(
            "CHINESE_HONG_KONG_SAR",
            3076,
            "zh-HK",
            "The Chinese Hong Kong S" "AR language.",
        ),
        XmlMappedEnumMember(
            "CHINESE_MACAO_SAR", 5124, "zh-MO", "The Chinese Macao SAR langu" "age."
        ),
        XmlMappedEnumMember(
            "CHINESE_SINGAPORE", 4100, "zh-SG", "The Chinese Singapore langu" "age."
        ),
        XmlMappedEnumMember("CROATIAN", 1050, "hr-HR", "The Croatian language."),
        XmlMappedEnumMember("CZECH", 1029, "cs-CZ", "The Czech language."),
        XmlMappedEnumMember("DANISH", 1030, "da-DK", "The Danish language."),
        XmlMappedEnumMember("DIVEHI", 1125, "div-MV", "The Divehi language."),
        XmlMappedEnumMember("DUTCH", 1043, "nl-NL", "The Dutch language."),
        XmlMappedEnumMember("EDO", 1126, "bin-NG", "The Edo language."),
        XmlMappedEnumMember("ENGLISH_AUS", 3081, "en-AU", "The English AUS language."),
        XmlMappedEnumMember(
            "ENGLISH_BELIZE", 10249, "en-BZ", "The English Belize language."
        ),
        XmlMappedEnumMember(
            "ENGLISH_CANADIAN", 4105, "en-CA", "The English Canadian languag" "e."
        ),
        XmlMappedEnumMember(
            "ENGLISH_CARIBBEAN", 9225, "en-CB", "The English Caribbean langu" "age."
        ),
        XmlMappedEnumMember(
            "ENGLISH_INDONESIA", 14345, "en-ID", "The English Indonesia lang" "uage."
        ),
        XmlMappedEnumMember(
            "ENGLISH_IRELAND", 6153, "en-IE", "The English Ireland language."
        ),
        XmlMappedEnumMember(
            "ENGLISH_JAMAICA", 8201, "en-JA", "The English Jamaica language."
        ),
        XmlMappedEnumMember(
            "ENGLISH_NEW_ZEALAND", 5129, "en-NZ", "The English NewZealand la" "nguage."
        ),
        XmlMappedEnumMember(
            "ENGLISH_PHILIPPINES",
            13321,
            "en-PH",
            "The English Philippines " "language.",
        ),
        XmlMappedEnumMember(
            "ENGLISH_SOUTH_AFRICA",
            7177,
            "en-ZA",
            "The English South Africa" " language.",
        ),
        XmlMappedEnumMember(
            "ENGLISH_TRINIDAD_TOBAGO",
            11273,
            "en-TT",
            "The English Trinidad" " Tobago language.",
        ),
        XmlMappedEnumMember("ENGLISH_UK", 2057, "en-GB", "The English UK language."),
        XmlMappedEnumMember("ENGLISH_US", 1033, "en-US", "The English US language."),
        XmlMappedEnumMember(
            "ENGLISH_ZIMBABWE", 12297, "en-ZW", "The English Zimbabwe langua" "ge."
        ),
        XmlMappedEnumMember("ESTONIAN", 1061, "et-EE", "The Estonian language."),
        XmlMappedEnumMember("FAEROESE", 1080, "fo-FO", "The Faeroese language."),
        XmlMappedEnumMember("FARSI", 1065, "fa-IR", "The Farsi language."),
        XmlMappedEnumMember("FILIPINO", 1124, "fil-PH", "The Filipino language."),
        XmlMappedEnumMember("FINNISH", 1035, "fi-FI", "The Finnish language."),
        XmlMappedEnumMember(
            "FRANCH_CONGO_DRC", 9228, "fr-CD", "The French Congo DRC languag" "e."
        ),
        XmlMappedEnumMember("FRENCH", 1036, "fr-FR", "The French language."),
        XmlMappedEnumMember(
            "FRENCH_CAMEROON", 11276, "fr-CM", "The French Cameroon language" "."
        ),
        XmlMappedEnumMember(
            "FRENCH_CANADIAN", 3084, "fr-CA", "The French Canadian language."
        ),
        XmlMappedEnumMember(
            "FRENCH_COTED_IVOIRE",
            12300,
            "fr-CI",
            "The French Coted Ivoire " "language.",
        ),
        XmlMappedEnumMember(
            "FRENCH_HAITI", 15372, "fr-HT", "The French Haiti language."
        ),
        XmlMappedEnumMember(
            "FRENCH_LUXEMBOURG", 5132, "fr-LU", "The French Luxembourg langu" "age."
        ),
        XmlMappedEnumMember("FRENCH_MALI", 13324, "fr-ML", "The French Mali language."),
        XmlMappedEnumMember(
            "FRENCH_MONACO", 6156, "fr-MC", "The French Monaco language."
        ),
        XmlMappedEnumMember(
            "FRENCH_MOROCCO", 14348, "fr-MA", "The French Morocco language."
        ),
        XmlMappedEnumMember(
            "FRENCH_REUNION", 8204, "fr-RE", "The French Reunion language."
        ),
        XmlMappedEnumMember(
            "FRENCH_SENEGAL", 10252, "fr-SN", "The French Senegal language."
        ),
        XmlMappedEnumMember(
            "FRENCH_WEST_INDIES",
            7180,
            "fr-WINDIES",
            "The French West Indie" "s language.",
        ),
        XmlMappedEnumMember(
            "FRISIAN_NETHERLANDS", 1122, "fy-NL", "The Frisian Netherlands l" "anguage."
        ),
        XmlMappedEnumMember("FULFULDE", 1127, "ff-NG", "The Fulfulde language."),
        XmlMappedEnumMember(
            "GAELIC_IRELAND", 2108, "ga-IE", "The Gaelic Ireland language."
        ),
        XmlMappedEnumMember(
            "GAELIC_SCOTLAND", 1084, "en-US", "The Gaelic Scotland language."
        ),
        XmlMappedEnumMember("GALICIAN", 1110, "gl-ES", "The Galician language."),
        XmlMappedEnumMember("GEORGIAN", 1079, "ka-GE", "The Georgian language."),
        XmlMappedEnumMember("GERMAN", 1031, "de-DE", "The German language."),
        XmlMappedEnumMember(
            "GERMAN_AUSTRIA", 3079, "de-AT", "The German Austria language."
        ),
        XmlMappedEnumMember(
            "GERMAN_LIECHTENSTEIN",
            5127,
            "de-LI",
            "The German Liechtenstein" " language.",
        ),
        XmlMappedEnumMember(
            "GERMAN_LUXEMBOURG", 4103, "de-LU", "The German Luxembourg langu" "age."
        ),
        XmlMappedEnumMember("GREEK", 1032, "el-GR", "The Greek language."),
        XmlMappedEnumMember("GUARANI", 1140, "gn-PY", "The Guarani language."),
        XmlMappedEnumMember("GUJARATI", 1095, "gu-IN", "The Gujarati language."),
        XmlMappedEnumMember("HAUSA", 1128, "ha-NG", "The Hausa language."),
        XmlMappedEnumMember("HAWAIIAN", 1141, "haw-US", "The Hawaiian language."),
        XmlMappedEnumMember("HEBREW", 1037, "he-IL", "The Hebrew language."),
        XmlMappedEnumMember("HINDI", 1081, "hi-IN", "The Hindi language."),
        XmlMappedEnumMember("HUNGARIAN", 1038, "hu-HU", "The Hungarian language."),
        XmlMappedEnumMember("IBIBIO", 1129, "ibb-NG", "The Ibibio language."),
        XmlMappedEnumMember("ICELANDIC", 1039, "is-IS", "The Icelandic language."),
        XmlMappedEnumMember("IGBO", 1136, "ig-NG", "The Igbo language."),
        XmlMappedEnumMember("INDONESIAN", 1057, "id-ID", "The Indonesian language."),
        XmlMappedEnumMember("INUKTITUT", 1117, "iu-Cans-CA", "The Inuktitut language."),
        XmlMappedEnumMember("ITALIAN", 1040, "it-IT", "The Italian language."),
        XmlMappedEnumMember("JAPANESE", 1041, "ja-JP", "The Japanese language."),
        XmlMappedEnumMember("KANNADA", 1099, "kn-IN", "The Kannada language."),
        XmlMappedEnumMember("KANURI", 1137, "kr-NG", "The Kanuri language."),
        XmlMappedEnumMember("KASHMIRI", 1120, "ks-Arab", "The Kashmiri language."),
        XmlMappedEnumMember(
            "KASHMIRI_DEVANAGARI",
            2144,
            "ks-Deva",
            "The Kashmiri Devanagari" " language.",
        ),
        XmlMappedEnumMember("KAZAKH", 1087, "kk-KZ", "The Kazakh language."),
        XmlMappedEnumMember("KHMER", 1107, "kh-KH", "The Khmer language."),
        XmlMappedEnumMember("KIRGHIZ", 1088, "ky-KG", "The Kirghiz language."),
        XmlMappedEnumMember("KONKANI", 1111, "kok-IN", "The Konkani language."),
        XmlMappedEnumMember("KOREAN", 1042, "ko-KR", "The Korean language."),
        XmlMappedEnumMember("KYRGYZ", 1088, "ky-KG", "The Kyrgyz language."),
        XmlMappedEnumMember("LAO", 1108, "lo-LA", "The Lao language."),
        XmlMappedEnumMember("LATIN", 1142, "la-Latn", "The Latin language."),
        XmlMappedEnumMember("LATVIAN", 1062, "lv-LV", "The Latvian language."),
        XmlMappedEnumMember("LITHUANIAN", 1063, "lt-LT", "The Lithuanian language."),
        XmlMappedEnumMember(
            "MACEDONINAN_FYROM", 1071, "mk-MK", "The Macedonian FYROM langua" "ge."
        ),
        XmlMappedEnumMember(
            "MALAY_BRUNEI_DARUSSALAM",
            2110,
            "ms-BN",
            "The Malay Brunei Daru" "ssalam language.",
        ),
        XmlMappedEnumMember("MALAYALAM", 1100, "ml-IN", "The Malayalam language."),
        XmlMappedEnumMember("MALAYSIAN", 1086, "ms-MY", "The Malaysian language."),
        XmlMappedEnumMember("MALTESE", 1082, "mt-MT", "The Maltese language."),
        XmlMappedEnumMember("MANIPURI", 1112, "mni-IN", "The Manipuri language."),
        XmlMappedEnumMember("MAORI", 1153, "mi-NZ", "The Maori language."),
        XmlMappedEnumMember("MARATHI", 1102, "mr-IN", "The Marathi language."),
        XmlMappedEnumMember(
            "MEXICAN_SPANISH", 2058, "es-MX", "The Mexican Spanish language."
        ),
        XmlMappedEnumMember("MONGOLIAN", 1104, "mn-MN", "The Mongolian language."),
        XmlMappedEnumMember("NEPALI", 1121, "ne-NP", "The Nepali language."),
        XmlMappedEnumMember("NO_PROOFING", 1024, "en-US", "No proofing."),
        XmlMappedEnumMember(
            "NORWEGIAN_BOKMOL", 1044, "nb-NO", "The Norwegian Bokmol languag" "e."
        ),
        XmlMappedEnumMember(
            "NORWEGIAN_NYNORSK", 2068, "nn-NO", "The Norwegian Nynorsk langu" "age."
        ),
        XmlMappedEnumMember("ORIYA", 1096, "or-IN", "The Oriya language."),
        XmlMappedEnumMember("OROMO", 1138, "om-Ethi-ET", "The Oromo language."),
        XmlMappedEnumMember("PASHTO", 1123, "ps-AF", "The Pashto language."),
        XmlMappedEnumMember("POLISH", 1045, "pl-PL", "The Polish language."),
        XmlMappedEnumMember("PORTUGUESE", 2070, "pt-PT", "The Portuguese language."),
        XmlMappedEnumMember("PUNJABI", 1094, "pa-IN", "The Punjabi language."),
        XmlMappedEnumMember(
            "QUECHUA_BOLIVIA", 1131, "quz-BO", "The Quechua Bolivia language" "."
        ),
        XmlMappedEnumMember(
            "QUECHUA_ECUADOR", 2155, "quz-EC", "The Quechua Ecuador language" "."
        ),
        XmlMappedEnumMember(
            "QUECHUA_PERU", 3179, "quz-PE", "The Quechua Peru language."
        ),
        XmlMappedEnumMember(
            "RHAETO_ROMANIC", 1047, "rm-CH", "The Rhaeto Romanic language."
        ),
        XmlMappedEnumMember("ROMANIAN", 1048, "ro-RO", "The Romanian language."),
        XmlMappedEnumMember(
            "ROMANIAN_MOLDOVA", 2072, "ro-MO", "The Romanian Moldova languag" "e."
        ),
        XmlMappedEnumMember("RUSSIAN", 1049, "ru-RU", "The Russian language."),
        XmlMappedEnumMember(
            "RUSSIAN_MOLDOVA", 2073, "ru-MO", "The Russian Moldova language."
        ),
        XmlMappedEnumMember(
            "SAMI_LAPPISH", 1083, "se-NO", "The Sami Lappish language."
        ),
        XmlMappedEnumMember("SANSKRIT", 1103, "sa-IN", "The Sanskrit language."),
        XmlMappedEnumMember("SEPEDI", 1132, "ns-ZA", "The Sepedi language."),
        XmlMappedEnumMember(
            "SERBIAN_BOSNIA_HERZEGOVINA_CYRILLIC",
            7194,
            "sr-BA",
            "The Serbi" "an Bosnia Herzegovina Cyrillic language.",
        ),
        XmlMappedEnumMember(
            "SERBIAN_BOSNIA_HERZEGOVINA_LATIN",
            6170,
            "sr-Latn-BA",
            "The Ser" "bian Bosnia Herzegovina Latin language.",
        ),
        XmlMappedEnumMember(
            "SERBIAN_CYRILLIC", 3098, "sr-SP", "The Serbian Cyrillic languag" "e."
        ),
        XmlMappedEnumMember(
            "SERBIAN_LATIN", 2074, "sr-Latn-CS", "The Serbian Latin language" "."
        ),
        XmlMappedEnumMember("SESOTHO", 1072, "st-ZA", "The Sesotho language."),
        XmlMappedEnumMember(
            "SIMPLIFIED_CHINESE", 2052, "zh-CN", "The Simplified Chinese lan" "guage."
        ),
        XmlMappedEnumMember("SINDHI", 1113, "sd-Deva-IN", "The Sindhi language."),
        XmlMappedEnumMember(
            "SINDHI_PAKISTAN", 2137, "sd-Arab-PK", "The Sindhi Pakistan lang" "uage."
        ),
        XmlMappedEnumMember("SINHALESE", 1115, "si-LK", "The Sinhalese language."),
        XmlMappedEnumMember("SLOVAK", 1051, "sk-SK", "The Slovak language."),
        XmlMappedEnumMember("SLOVENIAN", 1060, "sl-SI", "The Slovenian language."),
        XmlMappedEnumMember("SOMALI", 1143, "so-SO", "The Somali language."),
        XmlMappedEnumMember("SORBIAN", 1070, "wen-DE", "The Sorbian language."),
        XmlMappedEnumMember("SPANISH", 1034, "es-ES_tradnl", "The Spanish language."),
        XmlMappedEnumMember(
            "SPANISH_ARGENTINA", 11274, "es-AR", "The Spanish Argentina lang" "uage."
        ),
        XmlMappedEnumMember(
            "SPANISH_BOLIVIA", 16394, "es-BO", "The Spanish Bolivia language" "."
        ),
        XmlMappedEnumMember(
            "SPANISH_CHILE", 13322, "es-CL", "The Spanish Chile language."
        ),
        XmlMappedEnumMember(
            "SPANISH_COLOMBIA", 9226, "es-CO", "The Spanish Colombia languag" "e."
        ),
        XmlMappedEnumMember(
            "SPANISH_COSTA_RICA", 5130, "es-CR", "The Spanish Costa Rica lan" "guage."
        ),
        XmlMappedEnumMember(
            "SPANISH_DOMINICAN_REPUBLIC",
            7178,
            "es-DO",
            "The Spanish Domini" "can Republic language.",
        ),
        XmlMappedEnumMember(
            "SPANISH_ECUADOR", 12298, "es-EC", "The Spanish Ecuador language" "."
        ),
        XmlMappedEnumMember(
            "SPANISH_EL_SALVADOR",
            17418,
            "es-SV",
            "The Spanish El Salvador " "language.",
        ),
        XmlMappedEnumMember(
            "SPANISH_GUATEMALA", 4106, "es-GT", "The Spanish Guatemala langu" "age."
        ),
        XmlMappedEnumMember(
            "SPANISH_HONDURAS", 18442, "es-HN", "The Spanish Honduras langua" "ge."
        ),
        XmlMappedEnumMember(
            "SPANISH_MODERN_SORT", 3082, "es-ES", "The Spanish Modern Sort l" "anguage."
        ),
        XmlMappedEnumMember(
            "SPANISH_NICARAGUA", 19466, "es-NI", "The Spanish Nicaragua lang" "uage."
        ),
        XmlMappedEnumMember(
            "SPANISH_PANAMA", 6154, "es-PA", "The Spanish Panama language."
        ),
        XmlMappedEnumMember(
            "SPANISH_PARAGUAY", 15370, "es-PY", "The Spanish Paraguay langua" "ge."
        ),
        XmlMappedEnumMember(
            "SPANISH_PERU", 10250, "es-PE", "The Spanish Peru language."
        ),
        XmlMappedEnumMember(
            "SPANISH_PUERTO_RICO",
            20490,
            "es-PR",
            "The Spanish Puerto Rico " "language.",
        ),
        XmlMappedEnumMember(
            "SPANISH_URUGUAY", 14346, "es-UR", "The Spanish Uruguay language" "."
        ),
        XmlMappedEnumMember(
            "SPANISH_VENEZUELA", 8202, "es-VE", "The Spanish Venezuela langu" "age."
        ),
        XmlMappedEnumMember("SUTU", 1072, "st-ZA", "The Sutu language."),
        XmlMappedEnumMember("SWAHILI", 1089, "sw-KE", "The Swahili language."),
        XmlMappedEnumMember("SWEDISH", 1053, "sv-SE", "The Swedish language."),
        XmlMappedEnumMember(
            "SWEDISH_FINLAND", 2077, "sv-FI", "The Swedish Finland language."
        ),
        XmlMappedEnumMember(
            "SWISS_FRENCH", 4108, "fr-CH", "The Swiss French language."
        ),
        XmlMappedEnumMember(
            "SWISS_GERMAN", 2055, "de-CH", "The Swiss German language."
        ),
        XmlMappedEnumMember(
            "SWISS_ITALIAN", 2064, "it-CH", "The Swiss Italian language."
        ),
        XmlMappedEnumMember("SYRIAC", 1114, "syr-SY", "The Syriac language."),
        XmlMappedEnumMember("TAJIK", 1064, "tg-TJ", "The Tajik language."),
        XmlMappedEnumMember(
            "TAMAZIGHT", 1119, "tzm-Arab-MA", "The Tamazight language."
        ),
        XmlMappedEnumMember(
            "TAMAZIGHT_LATIN", 2143, "tmz-DZ", "The Tamazight Latin language" "."
        ),
        XmlMappedEnumMember("TAMIL", 1097, "ta-IN", "The Tamil language."),
        XmlMappedEnumMember("TATAR", 1092, "tt-RU", "The Tatar language."),
        XmlMappedEnumMember("TELUGU", 1098, "te-IN", "The Telugu language."),
        XmlMappedEnumMember("THAI", 1054, "th-TH", "The Thai language."),
        XmlMappedEnumMember("TIBETAN", 1105, "bo-CN", "The Tibetan language."),
        XmlMappedEnumMember(
            "TIGRIGNA_ERITREA", 2163, "ti-ER", "The Tigrigna Eritrea languag" "e."
        ),
        XmlMappedEnumMember(
            "TIGRIGNA_ETHIOPIC", 1139, "ti-ET", "The Tigrigna Ethiopic langu" "age."
        ),
        XmlMappedEnumMember(
            "TRADITIONAL_CHINESE", 1028, "zh-TW", "The Traditional Chinese l" "anguage."
        ),
        XmlMappedEnumMember("TSONGA", 1073, "ts-ZA", "The Tsonga language."),
        XmlMappedEnumMember("TSWANA", 1074, "tn-ZA", "The Tswana language."),
        XmlMappedEnumMember("TURKISH", 1055, "tr-TR", "The Turkish language."),
        XmlMappedEnumMember("TURKMEN", 1090, "tk-TM", "The Turkmen language."),
        XmlMappedEnumMember("UKRAINIAN", 1058, "uk-UA", "The Ukrainian language."),
        XmlMappedEnumMember("URDU", 1056, "ur-PK", "The Urdu language."),
        XmlMappedEnumMember(
            "UZBEK_CYRILLIC", 2115, "uz-UZ", "The Uzbek Cyrillic language."
        ),
        XmlMappedEnumMember(
            "UZBEK_LATIN", 1091, "uz-Latn-UZ", "The Uzbek Latin language."
        ),
        XmlMappedEnumMember("VENDA", 1075, "ve-ZA", "The Venda language."),
        XmlMappedEnumMember("VIETNAMESE", 1066, "vi-VN", "The Vietnamese language."),
        XmlMappedEnumMember("WELSH", 1106, "cy-GB", "The Welsh language."),
        XmlMappedEnumMember("XHOSA", 1076, "xh-ZA", "The Xhosa language."),
        XmlMappedEnumMember("YI", 1144, "ii-CN", "The Yi language."),
        XmlMappedEnumMember("YIDDISH", 1085, "yi-Hebr", "The Yiddish language."),
        XmlMappedEnumMember("YORUBA", 1130, "yo-NG", "The Yoruba language."),
        XmlMappedEnumMember("ZULU", 1077, "zu-ZA", "The Zulu language."),
    )
