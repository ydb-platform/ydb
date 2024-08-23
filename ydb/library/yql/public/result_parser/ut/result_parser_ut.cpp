#include <library/cpp/testing/unittest/registar.h>

#include "result_parser.h"

using namespace NYql;

Y_UNIT_TEST_SUITE(TResultParserTest) {

    Y_UNIT_TEST(NotImplemented) {
        const auto yson = R"(
[
    {
        "Label" = "xxx";
        "Position" = {
            "File" = "<main>";
            "Row" = 5;
            "Column" = 1
        };
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "value";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "key";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "subkey";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        "value:aaa";
                        "023";
                        ""
                    ];
                    [
                        "value:ddd";
                        "037";
                        ""
                    ];
                    [
                        "value:abc";
                        "075";
                        ""
                    ]
                ]
            }
        ]
    };
    {
        "Label" = "yyy";
        "Position" = {
            "File" = "<main>";
            "Row" = 15;
            "Column" = 1
        };
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "key";
                                [
                                    "OptionalType";
                                    [
                                        "DataType";
                                        "Int32"
                                    ]
                                ]
                            ];
                            [
                                "value";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        [
                            "911"
                        ];
                        "kkk"
                    ];
                    [
                        [
                            "761"
                        ];
                        "ccc"
                    ];
                    [
                        [
                            "527"
                        ];
                        "bbb"
                    ];
                    [
                        [
                            "200"
                        ];
                        "qqq"
                    ];
                    [
                        [
                            "150"
                        ];
                        "aaa"
                    ];
                    [
                        [
                            "150"
                        ];
                        "iii"
                    ];
                    [
                        [
                            "150"
                        ];
                        "zzz"
                    ];
                    [
                        [
                            "75"
                        ];
                        "abc"
                    ];
                    [
                        [
                            "37"
                        ];
                        "ddd"
                    ];
                    [
                        [
                            "23"
                        ];
                        "aaa"
                    ]
                ]
            }
        ]
    };
    {
        "Label" = "zzz";
        "Position" = {
            "File" = "<main>";
            "Row" = 22;
            "Column" = 1
        };
        "Write" = [
            {
                "Type" = [
                    "ListType";
                    [
                        "StructType";
                        [
                            [
                                "column0";
                                [
                                    "OptionalType";
                                    [
                                        "DataType";
                                        "Decimal";
                                        "10";
                                        "5"
                                    ]
                                ]
                            ]
                        ]
                    ]
                ];
                "Data" = [
                    [
                        [
                            "3.14"
                        ]
                    ]
                ]
            }
        ]
    }        )";

        TResultVisitorBase visitor;
        UNIT_ASSERT_EXCEPTION(ParseResult(yson, visitor), yexception);
    }
}
