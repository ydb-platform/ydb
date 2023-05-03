#include "mkql_proto.h"

#include <ydb/library/mkql_proto/ut/helpers/helpers.h>

using namespace std::string_view_literals;

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLProtoTest) {

    Y_UNIT_TEST(TestCanExport) {
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        UNIT_ASSERT(!CanExportType(pgmBuilder.NewVoid().GetStaticType()->GetType(), env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewVoid().GetStaticType(), env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewPgType(16), env));
        auto dtype = pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT(CanExportType(dtype, env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewOptionalType(dtype), env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewListType(dtype), env));
        TVector<TType *> dtype1{dtype};
        UNIT_ASSERT(CanExportType(pgmBuilder.NewTupleType(dtype1), env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewStructType(pgmBuilder.NewEmptyStructType(), "x", dtype), env));
        UNIT_ASSERT(CanExportType(pgmBuilder.NewDictType(dtype, dtype, false), env));
        UNIT_ASSERT(!CanExportType(pgmBuilder.NewDictType(dtype, dtype->GetType(), false), env));
    }

    Y_UNIT_TEST(TestExportVoidType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder &pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewVoid();
            return pgmReturn;
        }, "Kind: Void\n");
    }

    Y_UNIT_TEST(TestExportNullType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder &pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewNull();
            return pgmReturn;
        }, "Kind: Null\n");
    }

    Y_UNIT_TEST(TestExportDataType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<i32>(42);
            return pgmReturn;
        },
        "Kind: Data\n"
        "Data {\n"
        "  Scheme: 1\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportDecimalType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 1;
            p[1] = 0;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        },
        "Kind: Data\n"
        "Data {\n"
        "  Scheme: 4865\n"
        "  DecimalParams {\n"
        "    Precision: 21\n"
        "    Scale: 8\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportPgType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgType = static_cast<TPgType*>(pgmBuilder.NewPgType(16));
            auto pgmReturn = pgmBuilder.PgConst(pgType, "true");
            return pgmReturn;
        },
        "Kind: Pg\n"
        "Pg {\n"
        "  oid: 16\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportUuidType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Uuid>(TStringBuf("\1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"sv));
            return pgmReturn;
        },
        "Kind: Data\n"
        "Data {\n"
        "  Scheme: 4611\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportOptionalType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<i32>(42));
            return pgmReturn;
        },
        "Kind: Optional\n"
        "Optional {\n"
        "  Item {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 1\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportOptionalType2) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
            return pgmReturn;
        },
        "Kind: Optional\n"
        "Optional {\n"
        "  Item {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 4097\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportOptionalOptionalType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id));
            return pgmReturn;
        },
        "Kind: Optional\n"
        "Optional {\n"
        "  Item {\n"
        "    Kind: Optional\n"
        "    Optional {\n"
        "      Item {\n"
        "        Kind: Data\n"
        "        Data {\n"
        "          Scheme: 4097\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportEmptyListType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyList();
            return pgmReturn;
        },
        "Kind: EmptyList\n");
    }

    Y_UNIT_TEST(TestExportListType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.AsList(pgmBuilder.NewDataLiteral<i32>(42));
            return pgmReturn;
        },
        "Kind: List\n"
        "List {\n"
        "  Item {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 1\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportTupleType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            TVector<TRuntimeNode> items;
            items.push_back(pgmBuilder.NewDataLiteral<i32>(42));
            items.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "Kind: Tuple\n"
        "Tuple {\n"
        "  Element {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 1\n"
        "    }\n"
        "  }\n"
        "  Element {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 4097\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportEmptyTupleType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            TVector<TRuntimeNode> items;
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "Kind: Tuple\n");

        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            TVector<TRuntimeNode> items;
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "tuple_type {\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportStructType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "Kind: Struct\n"
        "Struct {\n"
        "  Member {\n"
        "    Name: \"x\"\n"
        "    Type {\n"
        "      Kind: Data\n"
        "      Data {\n"
        "        Scheme: 1\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "  Member {\n"
        "    Name: \"y\"\n"
        "    Type {\n"
        "      Kind: Data\n"
        "      Data {\n"
        "        Scheme: 4097\n"
        "      }\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportEmptyStructType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "Kind: Struct\n");

        TestExportType<Ydb::Type>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "struct_type {\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportEmptyDictType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyDict();
            return pgmReturn;
        },
        "Kind: EmptyDict\n");
    }

    Y_UNIT_TEST(TestExportDictType) {
        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<i32>::Id),
                pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
            TVector<std::pair<TRuntimeNode, TRuntimeNode>> items;
            items.push_back({ pgmBuilder.NewDataLiteral<i32>(42),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewDict(dictType, items);
            return pgmReturn;
        },
        "Kind: Dict\n"
        "Dict {\n"
        "  Key {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 1\n"
        "    }\n"
        "  }\n"
        "  Payload {\n"
        "    Kind: Data\n"
        "    Data {\n"
        "      Scheme: 4097\n"
        "    }\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportVariantTupleType) {
        const TString expected = R"___(Kind: Variant
Variant {
  TupleItems {
    Element {
      Kind: Data
      Data {
        Scheme: 4
      }
    }
    Element {
      Kind: Data
      Data {
        Scheme: 2
      }
    }
  }
}
)___";

        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElemenTypes;
            tupleElemenTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
            tupleElemenTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            auto pgmReturn = pgmBuilder.NewVariant(
                pgmBuilder.NewDataLiteral<ui32>(66),
                1,
                pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElemenTypes)));
            return pgmReturn;
        },
        expected);
    }

    Y_UNIT_TEST(TestExportVariantStructType) {
        const TString expected = R"___(Kind: Variant
Variant {
  StructItems {
    Member {
      Name: "a"
      Type {
        Kind: Data
        Data {
          Scheme: 4
        }
      }
    }
    Member {
      Name: "b"
      Type {
        Kind: Data
        Data {
          Scheme: 2
        }
      }
    }
  }
}
)___";

        TestExportType<NKikimrMiniKQL::TType>([](TProgramBuilder& pgmBuilder) {
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

    Y_UNIT_TEST(TestExportVoid) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewVoid();
            return pgmReturn;
        }, "");
    }

    Y_UNIT_TEST(TestExportBool) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral(true);
            return pgmReturn;
        }, "Bool: true\n");
    }

    Y_UNIT_TEST(TestExportIntegral) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<i32>(42);
            return pgmReturn;
        }, "Int32: 42\n");
    }

    Y_UNIT_TEST(TestExportDouble) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral(3.5);
            return pgmReturn;
        }, "Double: 3.5\n");
    }

    Y_UNIT_TEST(TestExportString) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc");
            return pgmReturn;
        }, "Bytes: \"abc\"\n");
    }

    Y_UNIT_TEST(TestExportUuid) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Uuid>(TStringBuf("\1\0\0\0\0\0\0\0\2\0\0\0\0\0\0\0"sv));
            return pgmReturn;
        }, "Low128: 1\n"
           "Hi128: 2\n");
    }

    Y_UNIT_TEST(TestExportDecimal) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 1;
            p[1] = 0;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "Low128: 1\n"
           "Hi128: 0\n");
    }

    Y_UNIT_TEST(TestImportDecimal) {
        const TString type =
            "Kind: Data\n"
            "Data {\n"
            "  Scheme: 4865\n"
            "  DecimalParams {\n"
            "    Precision: 21\n"
            "    Scale: 8\n"
            "  }\n"
            "}\n";
         const TString value =
            "Low128: 1\n"
            "Hi128: 0\n";

        TestImportParams<NKikimrMiniKQL::TParams, NKikimrMiniKQL::TType, NKikimrMiniKQL::TValue>(type, value);
    }

    Y_UNIT_TEST(TestExportDecimalNegative) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = Max<ui64>();
            p[1] = Max<ui64>();
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "Low128: 18446744073709551615\n"
           "Hi128: 18446744073709551615\n");
    }

    Y_UNIT_TEST(TestExportDecimalMax64bit) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = Max<ui64>();
            p[1] = 0;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "Low128: 18446744073709551615\n"
           "Hi128: 0\n");
    }

    Y_UNIT_TEST(TestExportDecimalHuge) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 0;
            p[1] = 1;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "Low128: 0\n"
           "Hi128: 1\n");
    }

    Y_UNIT_TEST(TestExportDecimalHugePlusOne) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            NYql::NDecimal::TInt128 x;
            ui64* p = (ui64*)&x;
            p[0] = 1;
            p[1] = 1;
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(x, 21, 8);
            return pgmReturn;
        }, "Low128: 1\n"
           "Hi128: 1\n");
    }

    Y_UNIT_TEST(TestExportDecimalNan) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(NYql::NDecimal::Nan(), 21, 8);
            return pgmReturn;
        }, "Low128: 3136633892082024449\n"
           "Hi128: 5421010862427522\n");
    }

    Y_UNIT_TEST(TestExportDecimalMunusInf) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewDecimalLiteral(-NYql::NDecimal::Inf(), 21, 8);
            return pgmReturn;
        }, "Low128: 15310110181627527168\n"
           "Hi128: 18441323062847124093\n");
    }

    Y_UNIT_TEST(TestExportNull) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewNull();
            return pgmReturn;
        }, "NullFlagValue: NULL_VALUE\n");
    }

    Y_UNIT_TEST(TestExportEmptyOptional) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id);
            return pgmReturn;
        }, "");
    }

    Y_UNIT_TEST(TestExportEmptyOptionalOptional) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(
                pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<char*>::Id));
            return pgmReturn;
        },
        "Optional {\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportOptional) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            return pgmReturn;
        },
        "Optional {\n"
        "  Bytes: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportEmptyList) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyList();
            return pgmReturn;
        },
        "");
    }

    Y_UNIT_TEST(TestExportList) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.AsList(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            return pgmReturn;
        },
        "List {\n"
        "  Bytes: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportTuple) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            TRuntimeNode::TList items;
            items.push_back(pgmBuilder.NewDataLiteral<i32>(42));
            items.push_back(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
            auto pgmReturn = pgmBuilder.NewTuple(items);
            return pgmReturn;
        },
        "Tuple {\n"
        "  Int32: 42\n"
        "}\n"
        "Tuple {\n"
        "  Bytes: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportStruct) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "Struct {\n"
        "  Int32: 42\n"
        "}\n"
        "Struct {\n"
        "  Bytes: \"abc\"\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportStructEmptyColumnOrder) {
        const TVector<ui32> emptyColumnOrder;
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        // struct fields are in alpha order
        "Struct {\n"
        "  Int32: 42\n"
        "}\n"
        "Struct {\n"
        "  Bytes: \"abc\"\n"
        "}\n",
        &emptyColumnOrder);
    }

    Y_UNIT_TEST(TestExportStructWithColumnOrder) {
        const TVector<ui32> columnOrder = {1, 0};
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items;
            items.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewStruct(items);
            return pgmReturn;
        },
        "Struct {\n"
        "  Bytes: \"abc\"\n"
        "}\n"
        "Struct {\n"
        "  Int32: 42\n"
        "}\n",
        &columnOrder);
    }

    Y_UNIT_TEST(TestExportStructColumnOrderAffectsTopLevelOnly) {
        const TVector<ui32> columnOrder = {1, 0};
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            std::vector<std::pair<std::string_view, TRuntimeNode>> items1;
            items1.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(42) });
            items1.push_back({ "y", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto struct1 = pgmBuilder.NewStruct(items1);

            std::vector<std::pair<std::string_view, TRuntimeNode>> items2;
            items2.push_back({ "x", pgmBuilder.NewDataLiteral<i32>(777) });
            items2.push_back({ "y", struct1 });
            auto pgmReturn = pgmBuilder.NewStruct(items2);
            return pgmReturn;
        },
        "Struct {\n"
        "  Struct {\n"
        "    Int32: 42\n"
        "  }\n"
        "  Struct {\n"
        "    Bytes: \"abc\"\n"
        "  }\n"
        "}\n"
        "Struct {\n"
        "  Int32: 777\n"
        "}\n",
        &columnOrder);
    }

    Y_UNIT_TEST(TestExportEmptyDict) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto pgmReturn = pgmBuilder.NewEmptyDict();
            return pgmReturn;
        },
        "");
    }

    Y_UNIT_TEST(TestExportDict) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            auto dictType = pgmBuilder.NewDictType(pgmBuilder.NewDataType(NUdf::TDataType<i32>::Id),
                pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id), false);
            TVector<std::pair<TRuntimeNode, TRuntimeNode>> items;
            items.push_back({ pgmBuilder.NewDataLiteral<i32>(42),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc") });
            auto pgmReturn = pgmBuilder.NewDict(dictType, items);
            return pgmReturn;
        },
        "Dict {\n"
        "  Key {\n"
        "    Int32: 42\n"
        "  }\n"
        "  Payload {\n"
        "    Bytes: \"abc\"\n"
        "  }\n"
        "}\n");
    }

    Y_UNIT_TEST(TestExportVariant) {
        TestExportValue<NKikimrMiniKQL::TValue>([](TProgramBuilder& pgmBuilder) {
            std::vector<TType*> tupleElemenTypes;
            tupleElemenTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
            tupleElemenTypes.push_back(pgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
            auto pgmReturn = pgmBuilder.NewVariant(
                pgmBuilder.NewDataLiteral<ui32>(66),
                1,
                pgmBuilder.NewVariantType(pgmBuilder.NewTupleType(tupleElemenTypes)));
            return pgmReturn;
        },
        "Optional {\n"
        "  Uint32: 66\n"
        "}\n"
        "VariantIndex: 1\n");
    }

    Y_UNIT_TEST(TestImportVariant) {
        const TString type = R"___(Kind: Variant
Variant {
  TupleItems {
    Element {
      Kind: Data
      Data {
        Scheme: 4
      }
    }
    Element {
      Kind: Data
      Data {
        Scheme: 2
      }
    }
  }
}
)___";

        const TString value = R"___(Optional {
  Uint32: 66
}
VariantIndex: 1
)___";

        TestImportParams<NKikimrMiniKQL::TParams, NKikimrMiniKQL::TType, NKikimrMiniKQL::TValue>(type, value);
    }

    Y_UNIT_TEST(TestImportUuid) {
        const TString type = R"___(Kind: Data
Data {
  Scheme: 4611
}
)___";


        const TString value = R"___(Low128: 1
Hi128: 2
)___";

        TestImportParams<NKikimrMiniKQL::TParams, NKikimrMiniKQL::TType, NKikimrMiniKQL::TValue>(type, value);
    }

}

}
