#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/utils/yql_panic.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TBindingsUtilsTests) {
    Y_UNIT_TEST(LoadBindingsFromJsonText) {
        THashMap<TString, NSQLTranslation::TTableBindingSettings> dst;

        TString jsonText = R"__({
            "customer": {
                "ClusterType": "s3",
                "schema": [
                    "StructType", 
                    [
                        ["c_acctbal", ["OptionalType", ["DataType", "Double"]]],
                        ["c_address", ["OptionalType", ["DataType", "String"]]],
                        ["c_comment", ["OptionalType", ["DataType", "String"]]],
                        ["c_custkey", ["OptionalType", ["DataType", "Int32"]]],
                        ["c_mktsegment", ["OptionalType", ["DataType", "String"]]],
                        ["c_name", ["OptionalType", ["DataType", "String"]]],
                        ["c_nationkey", ["OptionalType", ["DataType", "Int32"]]],
                        ["c_phone", ["OptionalType", ["DataType", "String"]]]
                    ]
                ],
                "path": "h/300M/customer/",
                "cluster": "yq-tpc-local",
                "format": "parquet"
            }
        })__";
        LoadBindings(dst, jsonText);

        UNIT_ASSERT_VALUES_EQUAL(dst.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dst.contains("customer"), true);
        auto& binding = dst["customer"];
        UNIT_ASSERT_VALUES_EQUAL(binding.ClusterType, "s3");
        UNIT_ASSERT_VALUES_EQUAL(binding.Settings["schema"], 
        R"__([
    "StructType";
    [
        [
            "c_acctbal";
            [
                "OptionalType";
                [
                    "DataType";
                    "Double"
                ]
            ]
        ];
        [
            "c_address";
            [
                "OptionalType";
                [
                    "DataType";
                    "String"
                ]
            ]
        ];
        [
            "c_comment";
            [
                "OptionalType";
                [
                    "DataType";
                    "String"
                ]
            ]
        ];
        [
            "c_custkey";
            [
                "OptionalType";
                [
                    "DataType";
                    "Int32"
                ]
            ]
        ];
        [
            "c_mktsegment";
            [
                "OptionalType";
                [
                    "DataType";
                    "String"
                ]
            ]
        ];
        [
            "c_name";
            [
                "OptionalType";
                [
                    "DataType";
                    "String"
                ]
            ]
        ];
        [
            "c_nationkey";
            [
                "OptionalType";
                [
                    "DataType";
                    "Int32"
                ]
            ]
        ];
        [
            "c_phone";
            [
                "OptionalType";
                [
                    "DataType";
                    "String"
                ]
            ]
        ]
    ]
])__");
        UNIT_ASSERT_VALUES_EQUAL(binding.Settings["path"], "h/300M/customer/");
        UNIT_ASSERT_VALUES_EQUAL(binding.Settings["cluster"], "yq-tpc-local");
        UNIT_ASSERT_VALUES_EQUAL(binding.Settings["format"], "parquet");
    }

    Y_UNIT_TEST(AppendBindingsFromJsonText) {
        THashMap<TString, NSQLTranslation::TTableBindingSettings> dst;
        LoadBindings(dst, R"__({"customer": {}})__");
        UNIT_ASSERT_VALUES_EQUAL(dst.size(), 1);
        LoadBindings(dst, R"__({"lineitem": {}})__");
        UNIT_ASSERT_VALUES_EQUAL(dst.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(dst.contains("customer"), true);
        UNIT_ASSERT_VALUES_EQUAL(dst.contains("lineitem"), true);
    };

    Y_UNIT_TEST(ExceptionOnBadJson) {
        THashMap<TString, NSQLTranslation::TTableBindingSettings> dst;
        UNIT_ASSERT_EXCEPTION(LoadBindings(dst, R"__({"customer": {})__"), TYqlPanic);
    }
}
