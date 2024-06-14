#include "mkql_proto.h"

#include <ydb/library/mkql_proto/ut/helpers/helpers.h>

#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <ydb/core/scheme_types/scheme_types_defs.h>
#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace std::string_view_literals;

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLProtoTestYdb) {
    Y_UNIT_TEST(TestExportVoidTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewVoid();
            return pgmReturn;
        }, "void_type: NULL_VALUE\n");
    }

    Y_UNIT_TEST(TestExportDataTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<i32>(42);
            return pgmReturn;
        },
        "type_id: INT32\n");
    }

    Y_UNIT_TEST(TestExportDecimalTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 1;
            p[1] = 0;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        },
        "decimal_type {\n"
        "  precision: 21\n"
        "  scale: 8\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportUuidTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Uuid>("\1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"sv);
            return pgmReturn;
        },
        "type_id: UUID\n");
    }

    Y_UNIT_TEST(TestExportOptionalTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<i32>(42));
            return pgmReturn;
        },
        "optional_type {\n"
        "  item {\n"
        "    type_id: INT32\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportListTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.AsList(pgmBuilder.NewDataLiteral<i32>(42));
            return pgmReturn;
        },
        "list_type {\n"
        "  item {\n"
        "    type_id: INT32\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportTupleTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            TRuntimeNode::TList items;
            items.push_back(pgmBuilder.NewDataLiteral<i32>(42));
            items.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "tuple_type {\n"
        "  elements {\n"
        "    type_id: INT32\n"
        "  }\n"
        "  elements {\n"
        "    type_id: STRING\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportStructTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "struct_type {\n"
        "  members {\n"
        "    name: \"x\"\n"
        "    type {\n"
        "      type_id: INT32\n"
        "    }\n"
        "  }\n"
        "  members {\n"
        "    name: \"y\"\n"
        "    type {\n"
        "      type_id: STRING\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportDictTypeYdb) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<i32>::Id),
                pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
            TVector<std::pair<TRuntimeNode, TRuntimeNode>> items;
            items.push_back({ pgmBuilder.NewDataLiteral<i32>(42),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewDict(dictType, items);
            return pgmReturn;
        },
        "dict_type {\n"
        "  key {\n"
        "    type_id: INT32\n"
        "  }\n"
        "  payload {\n"
        "    type_id: STRING\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportVariantTupleTypeYdb) {
        const TString expected = R"___(variant_type {
  tuple_items {
    elements {
      type_id: UINT64
    }
    elements {
      type_id: UINT32
    }
  }
}
)___";
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            auto pgmReturn = pgmBuilder.NewVariant(
                pgmBuilder.NewDataLiteral<ui32>(66),
                1,
                pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes)));
            return pgmReturn;
        },
        expected);
    }

Y_UNIT_TEST(TestExportVariantStructTypeYdb) {
        const TString expected = R"___(variant_type {
  struct_items {
    members {
      name: "a"
      type {
        type_id: UINT64
      }
    }
    members {
      name: "b"
      type {
        type_id: UINT32
      }
    }
  }
}
)___";

        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TType*>> structElemenTypes;
            structElemenTypes.push_back({"a", pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)});
            structElemenTypes.push_back({"b", pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id)});
            TType* structType = pgmBuilder.NewStructType(structElemenTypes);
            auto pgmReturn = pgmBuilder.NewVariant(
                pgmBuilder.NewDataLiteral<ui32>(66),
                "b",
                pgmBuilder.NewVariantType(structType));
            return pgmReturn;
        },
        expected);
    }

    Y_UNIT_TEST(TestExportVoidYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewVoid();
            return pgmReturn;
        }, "");
    }

    Y_UNIT_TEST(TestExportBoolYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral(true);
            return pgmReturn;
        }, "bool_value: true\n");
    }

    Y_UNIT_TEST(TestExportIntegralYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<i32>(42);
            return pgmReturn;
        }, "int32_value: 42\n");
    }

    Y_UNIT_TEST(TestExportDoubleYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral(3.5);
            return pgmReturn;
        }, "double_value: 3.5\n");
    }

    Y_UNIT_TEST(TestExportStringYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc");
            return pgmReturn;
        }, "bytes_value: \"abc\"\n");
    }

    Y_UNIT_TEST(TestExportUuidYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Uuid>("\1\0\0\0\0\0\0\0\2\0\0\0\0\0\0\0"sv);
            return pgmReturn;
        }, "low_128: 1\n"
           "high_128: 2\n");
    }

    Y_UNIT_TEST(TestExportDecimalYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 1;
            p[1] = 0;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "low_128: 1\n");
    }

    Y_UNIT_TEST(TestExportDecimalNegativeYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = Max<ui64>();
            p[1] = Max<ui64>();
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "low_128: 18446744073709551615\n"
           "high_128: 18446744073709551615\n");
    }

    Y_UNIT_TEST(TestExportDecimalHugeYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 0;
            p[1] = 1;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "low_128: 0\n"
           "high_128: 1\n");
    }

    Y_UNIT_TEST(TestExportEmptyOptionalYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
            return pgmReturn;
        }, "null_flag_value: NULL_VALUE\n");
    }

    Y_UNIT_TEST(TestExportEmptyOptionalOptionalYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id));
            return pgmReturn;
        },
        "nested_value {\n"
        "  null_flag_value: NULL_VALUE\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportMultipleOptionalNotEmptyYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc")));
            return pgmReturn;
        }, "bytes_value: \"abc\"\n");
    }

    Y_UNIT_TEST(TestExportOptionalYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            return pgmReturn;
        }, "bytes_value: \"abc\"\n");
    }

    Y_UNIT_TEST(TestExportListYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.AsList(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            return pgmReturn;
        },
        "items {\n"
        "  bytes_value: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportTupleYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            TRuntimeNode::TList items;
            items.push_back(pgmBuilder.NewDataLiteral<i32>(42));
            items.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "items {\n"
        "  int32_value: 42\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportStructYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "items {\n"
        "  int32_value: 42\n"
        "}\n"
        "items {\n"
        "  bytes_value: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportDictYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<i32>::Id),
                pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
            TVector<std::pair<TRuntimeNode, TRuntimeNode>> items;
            items.push_back({ pgmBuilder.NewDataLiteral<i32>(42),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewDict(dictType, items);
            return pgmReturn;
        },
        "pairs {\n"
        "  key {\n"
        "    int32_value: 42\n"
        "  }\n"
        "  payload {\n"
        "    bytes_value: \"abc\"\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportVariantYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            auto pgmReturn = pgmBuilder.NewVariant(
                pgmBuilder.NewDataLiteral<ui32>(66),
                1,
                pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes)));
            return pgmReturn;
        },
        "nested_value {\n"
        "  uint32_value: 66\n"
        "}\n"
        "variant_index: 1\n");
    }

    Y_UNIT_TEST(TestExportMultipleOptionalVariantNotNullYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));

            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewOptional(pgmBuilder.NewVariant(
                    pgmBuilder.NewDataLiteral<ui32>(66),
                    1,
                    pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes)))));
            return pgmReturn;
        },
        "nested_value {\n"
        "  uint32_value: 66\n"
        "}\n"
        "variant_index: 1\n");
    }

    Y_UNIT_TEST(TestExportOptionalVariantOptionalNullYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)));

            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewVariant(
                    pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id),
                    1,
                    pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes))));
            return pgmReturn;
        },
        "nested_value {\n"
        "  nested_value {\n"
        "    null_flag_value: NULL_VALUE\n"
        "  }\n"
        "  variant_index: 1\n"
        "}\n");
    }


    Y_UNIT_TEST(TestExportMultipleOptionalVariantOptionalNullYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)));

            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewOptional(pgmBuilder.NewVariant(
                    pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id),
                    1,
                    pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes)))));
            return pgmReturn;
        },
        "nested_value {\n"
        "  nested_value {\n"
        "    nested_value {\n"
        "      null_flag_value: NULL_VALUE\n"
        "    }\n"
        "    variant_index: 1\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportMultipleOptionalVariantOptionalNotNullYdb) {
        TestExportValue<Ydb::Value>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)));

            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewOptional(pgmBuilder.NewVariant(
                    pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<ui64>(66)),
                    1,
                    pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes)))));
            return pgmReturn;
        },
        "nested_value {\n"
        "  uint64_value: 66\n"
        "}\n"
        "variant_index: 1\n");
    }

    Y_UNIT_TEST(TestExportOptionalVariantOptionalYdbType) {
        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElementTypes;
            tupleElementTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            tupleElementTypes.push_back(pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)));

            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewVariant(
                    pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id),
                    1,
                    pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElementTypes))));
            return pgmReturn;
        },
        "optional_type {\n"
        "  item {\n"
        "    variant_type {\n"
        "      tuple_items {\n"
        "        elements {\n"
        "          type_id: UINT32\n"
        "        }\n"
        "        elements {\n"
        "          optional_type {\n"
        "            item {\n"
        "              type_id: UINT64\n"
        "            }\n"
        "          }\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    TString DoTestCellsFromTuple(const TConstArrayRef<NScheme::TTypeInfo>& types, TString paramsProto) {
        NKikimrMiniKQL::TParams params;
        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(paramsProto, &params);
        UNIT_ASSERT_C(parseOk, paramsProto);

        TVector<TCell> cells;
        TVector<TString> memoryOwner;
        TString errStr;
        bool res = CellsFromTuple(&params.GetType(), params.GetValue(), types, {}, true, cells, errStr, memoryOwner);
        UNIT_ASSERT_VALUES_EQUAL_C(res, errStr.empty(), paramsProto);

        return errStr;
    }

    Y_UNIT_TEST(TestCellsFromTuple) {
        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Int32: -42 } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Value of type Int32 expected in tuple at position 0", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Int64: -42 } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Text : '-42' } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::String)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Text : 'AAAA' } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Cannot parse value of type Uint32 from text '-42' in tuple at position 0", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Uint32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Text : '-42' } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Tuple value length 0 doesn't match the length in type 1", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "        }"
            "    }"
            "    Value {"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Data must be present at position 0", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional {} }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Tuple value length 0 doesn't match the length in type 1", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32),
                NScheme::TTypeInfo(NScheme::NTypeIds::Utf8)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32),
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Int32 : -42 } }"
            "        Tuple { Optional { Text : '-42' } }"
            "    }"
            )
        );

        UNIT_ASSERT_VALUES_EQUAL("Tuple size 2 is greater that expected size 1", DoTestCellsFromTuple(
            {
                NScheme::TTypeInfo(NScheme::NTypeIds::Int32)
            },
            "   Type {"
            "        Kind : Tuple"
            "        Tuple {"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 1 } } } }"
            "            Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "        }"
            "    }"
            "    Value {"
            "        Tuple { Optional { Int32 : -42 } }"
            "        Tuple { Optional { Text : '-42' } }"
            "    }"
            )
        );
    }
}

}
}
