#include <ydb/library/yql/minikql/dom/json.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>

using namespace NYql;
using namespace NYql::NDom;
using namespace NKikimr;

constexpr char json[] =
R"(
{
    "Fullname": [
        {
            "freqs": {
                "sum_qf@de": 28,
                "sum_qf@en": 8,
                "sum_qf@ru": 10060,
                "sum_qf@tr": 91,
                "sum_qf@uk": 245,
                "sum_qf@uz": 6
            },
            "src": [
                {
                    "c": "ltr"
                }
            ],
            "value": "Татьяна Сорокина"
        }
    ],
    "Gender": [
        {
            "src": [
                {
                    "c": "yam",
                    "is_guessed": "True"
                },
                {
                    "c": "scm",
                    "is_guessed": "True"
                },
                {
                    "c": "ltr",
                    "is_guessed": "True"
                },
                {
                    "c": "lbr",
                    "is_guessed": "True"
                }
            ],
            "value": "female"
        }
    ],
    "Image": [
        {
            "RelevLocale": [
                "universe"
            ],
            "avatar_type": "face",
            "color_wiz": {
                "back": "#DBC4B5",
                "button": "#BFAC9E",
                "button_text": "#23211E",
                "text": "#705549"
            },
            "faces_count": 1,
            "langua": [
                "uk",
                "by",
                "kk",
                "ru"
            ],
            "mds_avatar_id": "2001742/402534297",
            "original_size": {
                "height": 1478,
                "width": 1478
            },
            "show_on_serp": true,
            "src": [
                {
                    "url": "http://music.yandex.ru/artist/7945920",
                    "url_type": "page",
                    "value": "yam"
                }
            ],
            "thumb": "Face",
            "type": "image",
            "url": "//avatars.yandex.net/get-music-content/113160/26f40ebf.a.8459289-1/orig",
            "value": "//avatars.yandex.net/get-music-content/113160/26f40ebf.a.8459289-1/orig"
        }
    ],
    "ImageSearchRequest": [
        {
            "RelevLocale": [
                "ru",
                "by"
            ],
            "value": "Сорокина Татьяна фото"
        }
    ],
    "Key": [
        {
            "langua": [
                "ru"
            ],
            "predict": "972",
            "rank": 0,
            "src": [
                {
                    "c": "rut"
                }
            ],
            "value": "sorokina tatyana"
        },
        {
            "freqs": {
                "sum_qf@de": 3,
                "sum_qf@en": 2,
                "sum_qf@ru": 11504,
                "sum_qf@tr": 35,
                "sum_qf@uk": 145,
                "sum_qf@uz": 1
            },
            "langua": [
                "ru"
            ],
            "src": [
                {
                    "c": "yam"
                },
                {
                    "c": "ltr"
                },
                {
                    "c": "lbr"
                }
            ],
            "value": "сорокина татьяна"
        },
        {
            "langua": [
                "ru"
            ],
            "predict": "931",
            "rank": 1,
            "src": [
                {
                    "c": "rut"
                }
            ],
            "value": "tatiana sorokina"
        },
        {
            "langua": [
                "ru"
            ],
            "predict": "951",
            "rank": 0,
            "src": [
                {
                    "c": "rut"
                }
            ],
            "value": "tatyana sorokina"
        },
        {
            "freqs": {
                "SenseRatio": 0.01,
                "SenseRatio@de": 0.5,
                "SenseRatio@en": 0.5,
                "SenseRatio@et": 0.5,
                "SenseRatio@fi": 0.5,
                "SenseRatio@id": 0.5,
                "SenseRatio@kk": 0.5,
                "SenseRatio@lt": 0.5,
                "SenseRatio@lv": 0.5,
                "SenseRatio@pl": 0.5,
                "SenseRatio@ru": 0,
                "SenseRatio@tr": 0.5,
                "SenseRatio@uk": 0.5,
                "SenseRatio@uz": 0.5,
                "sum_qf@de": 28,
                "sum_qf@en": 8,
                "sum_qf@ru": 10060,
                "sum_qf@tr": 91,
                "sum_qf@uk": 245,
                "sum_qf@uz": 6
            },
            "langua": [
                "ru"
            ],
            "src": [
                {
                    "c": "scm",
                    "name": "bookmate.com"
                },
                {
                    "c": "yam"
                },
                {
                    "c": "ltr"
                }
            ],
            "value": "татьяна сорокина"
        }
    ],
    "Projects": [
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845750|Наши дети]]"
                }
            ],
            "freqs": {
                "sum_qf": 256643
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845750|Наши дети]]"
        },
        {
            "Role": [
                "MAIN_ARTIST"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam08459289|Сто дорог, одна – моя]]"
                }
            ],
            "freqs": {
                "sum_qf": 54092
            },
            "hint_description": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "tr",
                        "by"
                    ],
                    "value": "2019"
                }
            ],
            "otype": "Music/Album@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam08459289|Сто дорог, одна – моя]]"
        },
        {
            "Role": "Author@on",
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm540668de..0|История медицины]]"
                }
            ],
            "freqs": {
                "sum_qf": 49611
            },
            "report": "False",
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm540668de..0|История медицины]]"
        },
        {
            "Role": "Author@on",
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm-3f1fcad4..0|Мыколка]]"
                }
            ],
            "report": "False",
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm-3f1fcad4..0|Мыколка]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845751|100 дорог]]"
                }
            ],
            "freqs": {
                "sum_qf": 21522
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845751|100 дорог]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#ltr08920335|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
                }
            ],
            "hint_description": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "tr",
                        "by"
                    ],
                    "value": "2015"
                }
            ],
            "report": "False",
            "src": [
                {
                    "c": "ltr"
                },
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#ltr08920335]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrbs464788|Пейп-арт]]"
                }
            ],
            "freqs": {
                "sum_qf": 12676
            },
            "report": "False",
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#lbrbs464788]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrbs137089|Филиальная сеть: развитие и управление]]"
                }
            ],
            "freqs": {
                "sum_qf": 21
            },
            "report": "False",
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#lbrbs137089]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb467470|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
                }
            ],
            "report": "False",
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb467470|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb464788|Пейп-арт]]"
                }
            ],
            "freqs": {
                "sum_qf": 12676
            },
            "report": "False",
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb464788|Пейп-арт]]"
        },
        {
            "Role": [
                "Artist@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb274279|Что сначала,что потом?]]"
                }
            ],
            "freqs": {
                "sum_qf": 15
            },
            "report": "False",
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb274279|Что сначала,что потом?]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb137089|Филиальная сеть: развитие и управление]]"
                }
            ],
            "freqs": {
                "sum_qf": 21
            },
            "report": "False",
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb137089|Филиальная сеть: развитие и управление]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845752|Храни его]]"
                }
            ],
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845752|Храни его]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845753|Удача]]"
                }
            ],
            "freqs": {
                "sum_qf": 1431963
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845753|Удача]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845754|Матушка Россия]]"
                }
            ],
            "freqs": {
                "sum_qf": 34699
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845754|Матушка Россия]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845755|Нежданное свидание]]"
                }
            ],
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845755|Нежданное свидание]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845756|Я – мама]]"
                }
            ],
            "freqs": {
                "sum_qf": 441
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845756|Я – мама]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845757|Глупый сон]]"
                }
            ],
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845757|Глупый сон]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845760|Спасибо вам]]"
                }
            ],
            "freqs": {
                "sum_qf": 152646
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845760|Спасибо вам]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845758|С Днём рождения]]"
                }
            ],
            "freqs": {
                "sum_qf": 16331217
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845758|С Днём рождения]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845759|Песенка о мечтах]]"
                }
            ],
            "freqs": {
                "sum_qf": 94
            },
            "otype": "Music/Recording@on",
            "report": "False",
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845759|Песенка о мечтах]]"
        },
        {
            "Role": "Author@on",
            "carousel": "False",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm-1eeb2744..0|Система дистрибуции: Инструменты создания конкурентного преимущества]]"
                }
            ],
            "report": "False",
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm-1eeb2744..0|Система дистрибуции: Инструменты создания конкурентного преимущества]]"
        }
    ],
    "SearchRequest": [
        {
            "RelevLocale": [
                "ru",
                "by"
            ],
            "value": "Сорокина Татьяна"
        }
    ],
    "Title": [
        {
            "freqs": {
                "sum_qf@de": 3,
                "sum_qf@en": 2,
                "sum_qf@ru": 11504,
                "sum_qf@tr": 35,
                "sum_qf@uk": 145,
                "sum_qf@uz": 1
            },
            "langua": [
                "ru"
            ],
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "Сорокина Татьяна"
        },
        {
            "RelevLocale": [
                "kz",
                "ua",
                "by",
                "ru"
            ],
            "freqs": {
                "sum_qf@de": 28,
                "sum_qf@en": 8,
                "sum_qf@ru": 10060,
                "sum_qf@tr": 91,
                "sum_qf@uk": 245,
                "sum_qf@uz": 6
            },
            "langua": [
                "ru"
            ],
            "src": [
                {
                    "c": "scm",
                    "name": "bookmate.com"
                },
                {
                    "c": "yam"
                },
                {
                    "c": "ltr"
                }
            ],
            "value": "Татьяна Сорокина"
        }
    ],
    "TopTracks": [
        {
            "Position": 9,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845757|Глупый сон]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845757|Глупый сон]]"
        },
        {
            "Position": 8,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845758|С Днём рождения]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845758|С Днём рождения]]"
        },
        {
            "Position": 7,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845759|Песенка о мечтах]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845759|Песенка о мечтах]]"
        },
        {
            "Position": 6,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845752|Храни его]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845752|Храни его]]"
        },
        {
            "Position": 5,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845753|Удача]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845753|Удача]]"
        },
        {
            "Position": 4,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845754|Матушка Россия]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845754|Матушка Россия]]"
        },
        {
            "Position": 3,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845755|Нежданное свидание]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845755|Нежданное свидание]]"
        },
        {
            "Position": 2,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845750|Наши дети]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845750|Наши дети]]"
        },
        {
            "Position": 1,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845751|100 дорог]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845751|100 дорог]]"
        },
        {
            "Position": 0,
            "RelevLocale": [
                "ru",
                "ua",
                "by",
                "kz"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845760|Спасибо вам]]"
                }
            ],
            "langua": [
                "uk",
                "ru",
                "kk",
                "by"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845760|Спасибо вам]]"
        }
    ],
    "freqs": {
        "average_proper_ratio": [
            "1.00"
        ],
        "proper_ratio": [
            {
                "src": [
                    {
                        "c": "yam"
                    }
                ],
                "value": "1.00"
            }
        ],
        "sum_qf@de": [
            "31"
        ],
        "sum_qf@en": [
            "10"
        ],
        "sum_qf@ru": [
            "21572"
        ],
        "sum_qf@tr": [
            "126"
        ],
        "sum_qf@uk": [
            "390"
        ],
        "sum_qf@uz": [
            "7"
        ]
    },
    "fullname": [
        {
            "freqs": {
                "sum_qf@de": 28,
                "sum_qf@en": 8,
                "sum_qf@ru": 10060,
                "sum_qf@tr": 91,
                "sum_qf@uk": 245,
                "sum_qf@uz": 6
            },
            "rfr": [
                "[[#rfr21731b2]]"
            ],
            "src": [
                {
                    "c": "ltr"
                }
            ],
            "value": "Татьяна Сорокина"
        }
    ],
    "human_gender": [
        {
            "rfr": [
                "[[#rfr21f0d779]]"
            ],
            "src": [
                {
                    "c": "yam",
                    "is_guessed": "True"
                },
                {
                    "c": "scm",
                    "is_guessed": "True"
                },
                {
                    "c": "ltr",
                    "is_guessed": "True"
                },
                {
                    "c": "lbr",
                    "is_guessed": "True"
                }
            ],
            "value": "female"
        }
    ],
    "ids": [
        {
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "http://music.yandex.ru/artist/7945920"
        },
        {
            "src": [
                {
                    "c": "ltr"
                }
            ],
            "value": "https://www.litres.ru/4815845"
        },
        {
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "http://www.labirint.ru/authors/43298"
        }
    ],
    "isa": {
        "Wtype": "Hum",
        "otype": [
            {
                "src": [
                    {
                        "c": "yam"
                    },
                    {
                        "c": "scm"
                    },
                    {
                        "c": "ltr"
                    },
                    {
                        "c": "lbr"
                    }
                ],
                "value": "Hum"
            }
        ]
    },
    "merged_ontoids": [
        "ltr24815845",
        "scmbookmatecomh7b363cfd07a49aed419fde3dbd010f64",
        "lbrh43298",
        "yam17945920"
    ],
    "musical_artist_groups": [
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845750|Наши дети]]"
                }
            ],
            "freqs": {
                "sum_qf": 256643
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845750|Наши дети]]"
        },
        {
            "Role": [
                "MAIN_ARTIST"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam08459289|Сто дорог, одна – моя]]"
                }
            ],
            "freqs": {
                "sum_qf": 54092
            },
            "hint_description": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "tr",
                        "by"
                    ],
                    "value": "2019"
                }
            ],
            "otype": "Music/Album@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam08459289|Сто дорог, одна – моя]]"
        },
        {
            "Role": "Author@on",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm540668de..0|История медицины]]"
                }
            ],
            "freqs": {
                "sum_qf": 49611
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm540668de..0|История медицины]]"
        },
        {
            "Role": "Author@on",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm-3f1fcad4..0|Мыколка]]"
                }
            ],
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm-3f1fcad4..0|Мыколка]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845751|100 дорог]]"
                }
            ],
            "freqs": {
                "sum_qf": 21522
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845751|100 дорог]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#ltr08920335|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
                }
            ],
            "hint_description": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "tr",
                        "by"
                    ],
                    "value": "2015"
                }
            ],
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "ltr"
                },
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#ltr08920335]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrbs464788|Пейп-арт]]"
                }
            ],
            "freqs": {
                "sum_qf": 12676
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#lbrbs464788]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrbs137089|Филиальная сеть: развитие и управление]]"
                }
            ],
            "freqs": {
                "sum_qf": 21
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr"
                }
            ],
            "value": "[[#lbrbs137089]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb467470|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
                }
            ],
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb467470|Система дистрибуции. Инструменты создания конкурентного преимущества]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb464788|Пейп-арт]]"
                }
            ],
            "freqs": {
                "sum_qf": 12676
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb464788|Пейп-арт]]"
        },
        {
            "Role": [
                "Artist@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb274279|Что сначала,что потом?]]"
                }
            ],
            "freqs": {
                "sum_qf": 15
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb274279|Что сначала,что потом?]]"
        },
        {
            "Role": [
                "Author@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#lbrb137089|Филиальная сеть: развитие и управление]]"
                }
            ],
            "freqs": {
                "sum_qf": 21
            },
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "lbr",
                    "f": "book"
                }
            ],
            "value": "[[#lbrb137089|Филиальная сеть: развитие и управление]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845752|Храни его]]"
                }
            ],
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845752|Храни его]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845753|Удача]]"
                }
            ],
            "freqs": {
                "sum_qf": 1431963
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845753|Удача]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845754|Матушка Россия]]"
                }
            ],
            "freqs": {
                "sum_qf": 34699
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845754|Матушка Россия]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845755|Нежданное свидание]]"
                }
            ],
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845755|Нежданное свидание]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845756|Я – мама]]"
                }
            ],
            "freqs": {
                "sum_qf": 441
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845756|Я – мама]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845757|Глупый сон]]"
                }
            ],
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845757|Глупый сон]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845760|Спасибо вам]]"
                }
            ],
            "freqs": {
                "sum_qf": 152646
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845760|Спасибо вам]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845758|С Днём рождения]]"
                }
            ],
            "freqs": {
                "sum_qf": 16331217
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845758|С Днём рождения]]"
        },
        {
            "Role": [
                "Performer@on"
            ],
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#yam356845759|Песенка о мечтах]]"
                }
            ],
            "freqs": {
                "sum_qf": 94
            },
            "otype": "Music/Recording@on",
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "yam"
                }
            ],
            "value": "[[#yam356845759|Песенка о мечтах]]"
        },
        {
            "Role": "Author@on",
            "formatted": [
                {
                    "RelevLocale": [
                        "ru",
                        "ua",
                        "by",
                        "kz"
                    ],
                    "value": "[[#scm-1eeb2744..0|Система дистрибуции: Инструменты создания конкурентного преимущества]]"
                }
            ],
            "rfr": [
                "[[#rfr110390d1]]"
            ],
            "src": [
                {
                    "c": "scm"
                }
            ],
            "value": "[[#scm-1eeb2744..0|Система дистрибуции: Инструменты создания конкурентного преимущества]]"
        }
    ]
}
)";

constexpr auto Steps = 10000U;

Y_UNIT_TEST_SUITE(TJsonTests) {
    Y_UNIT_TEST(TestValidate) {
        UNIT_ASSERT(IsValidJson(json));

        UNIT_ASSERT(!IsValidJson("[123}"));
        UNIT_ASSERT(!IsValidJson("[123],[456]"));
        UNIT_ASSERT(!IsValidJson(R"({"c" : "scm"])"));
        UNIT_ASSERT(!IsValidJson(""));
        UNIT_ASSERT(!IsValidJson(R"({"c",})"));
        UNIT_ASSERT(!IsValidJson(R"({null : "scm"})"));
        UNIT_ASSERT(!IsValidJson(R"({'one': 1})"));
    }

    Y_UNIT_TEST(TestPerfValidate) {
        const auto t = TInstant::Now();
        for (auto i = 0U; i < Steps; ++i) {
            UNIT_ASSERT(IsValidJson(json));
        }
        const auto time = TInstant::Now() - t;
        Cerr << "Time is " << time << Endl;
    }

    Y_UNIT_TEST(TestPerfParse) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TMemoryUsageInfo memInfo("Memory");
        NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, nullptr);
        NMiniKQL::TDefaultValueBuilder builder(holderFactory);

        std::array<NUdf::TUnboxedValue, Steps> v;

        const auto t = TInstant::Now();
        for (auto& i : v) {
            UNIT_ASSERT(i = TryParseJsonDom(json, &builder));
        }
        const auto time = TInstant::Now() - t;
        Cerr << "Time is " << time << Endl;
    }

    Y_UNIT_TEST(TestPerfSerialize) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TMemoryUsageInfo memInfo("Memory");
        NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, nullptr);
        NMiniKQL::TDefaultValueBuilder builder(holderFactory);

        const auto dom = TryParseJsonDom(json, &builder);
        std::array<NUdf::TUnboxedValue, Steps> v;

        const auto t = TInstant::Now();
        for (auto& i : v) {
            UNIT_ASSERT(i = builder.NewString(SerializeJsonDom(dom)));
        }
        const auto time = TInstant::Now() - t;
        Cerr << "Time is " << time << Endl;
    }
}
