#include "mkql_type_builder.h"

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/public/udf/udf_type_printer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/c/abi.h>

namespace NKikimr {

namespace NMiniKQL {

class TMiniKQLTypeBuilderTest : public TTestBase {
public:
    TMiniKQLTypeBuilderTest()
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , TypeInfoHelper(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder(Env, TypeInfoHelper, "", nullptr, {}) {
    }

private:
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder;

    UNIT_TEST_SUITE(TMiniKQLTypeBuilderTest);
        UNIT_TEST(TestPgTypeFormat);
        UNIT_TEST(TestSingularTypeFormat);
        UNIT_TEST(TestOptionalTypeFormat);
        UNIT_TEST(TestEnumTypeFormat);
        UNIT_TEST(TestResourceTypeFormat);
        UNIT_TEST(TestTaggedTypeFormat);
        UNIT_TEST(TestContainerTypeFormat);
        UNIT_TEST(TestStructTypeFormat);
        UNIT_TEST(TestTupleTypeFormat);
        UNIT_TEST(TestVariantTypeFormat);
        UNIT_TEST(TestCallableTypeFormat);
        UNIT_TEST(TestDataTypeFormat);
        UNIT_TEST(TestBlockTypeFormat);
        UNIT_TEST(TestArrowType);
    UNIT_TEST_SUITE_END();

    TString FormatType(NUdf::TType* t) {
        TStringBuilder output;
        NUdf::TTypePrinter p(*TypeInfoHelper, t);
        p.Out(output.Out);
        return output;
    }

    void TestPgTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Pg(NYql::NPg::LookupType("bool").TypeId));
            UNIT_ASSERT_VALUES_EQUAL(s, "pgbool");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Pg(NYql::NPg::LookupType("_bool").TypeId));
            UNIT_ASSERT_VALUES_EQUAL(s, "_pgbool");
        }
    }

    void TestBlockTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Block(true)->Item<ui32>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Scalar<Uint32>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Block(false)->Item<ui32>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Block<Uint32>");
        }
    }

    void TestSingularTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Null());
            UNIT_ASSERT_VALUES_EQUAL(s, "Null");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Void());
            UNIT_ASSERT_VALUES_EQUAL(s, "Void");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.EmptyList());
            UNIT_ASSERT_VALUES_EQUAL(s, "EmptyList");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.EmptyDict());
            UNIT_ASSERT_VALUES_EQUAL(s, "EmptyDict");
        }
    }

    void TestOptionalTypeFormat() {
        auto optional1 = FunctionTypeInfoBuilder.Optional()->Item<i64>().Build();
        auto optional2 = FunctionTypeInfoBuilder.Optional()->Item(optional1).Build();
        auto optional3 = FunctionTypeInfoBuilder.Optional()->Item(optional2).Build();
        {
            auto s = FormatType(optional1);
            UNIT_ASSERT_VALUES_EQUAL(s, "Int64?");
        }
        {
            auto s = FormatType(optional2);
            UNIT_ASSERT_VALUES_EQUAL(s, "Int64??");
        }
        {
            auto s = FormatType(optional3);
            UNIT_ASSERT_VALUES_EQUAL(s, "Int64???");
        }
    }

    void TestContainerTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.List()->Item<i8>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "List<Int8>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Dict()->Key<i8>().Value<bool>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Dict<Int8,Bool>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Set()->Key<char*>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Set<String>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Stream()->Item<ui64>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Stream<Uint64>");
        }
    }

    void TestEnumTypeFormat() {
        ui32 index = 0;
        auto t = FunctionTypeInfoBuilder.Enum(2)
            ->AddField("RED", &index)
            .AddField("GREEN", &index)
            .AddField("BLUE", &index)
            .Build();

        auto s = FormatType(t);
        UNIT_ASSERT_VALUES_EQUAL(s, "Enum<'BLUE','GREEN','RED'>");
    }

    void TestResourceTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Resource("my_resource"));
            UNIT_ASSERT_VALUES_EQUAL(s, "Resource<'my_resource'>");
        }
    }

    void TestTaggedTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Tagged(FunctionTypeInfoBuilder.SimpleType<i8>(), "my_resource"));
            UNIT_ASSERT_VALUES_EQUAL(s, "Tagged<Int8,'my_resource'>");
        }
    }

    void TestStructTypeFormat() {
        ui32 index = 0;
        auto s1 = FunctionTypeInfoBuilder.Struct(2)
            ->AddField<bool>("is_ok", &index)
            .AddField<char*>("name", &index)
            .Build();

        auto s2 = FunctionTypeInfoBuilder.Struct(3)
            ->AddField<char*>("country", &index)
            .AddField<ui64>("id", &index)
            .AddField("person", s1, &index)
            .Build();

        {
            auto s = FormatType(s1);
            UNIT_ASSERT_VALUES_EQUAL(s, "Struct<'is_ok':Bool,'name':String>");
        }
        {
            auto s = FormatType(s2);
            UNIT_ASSERT_VALUES_EQUAL(s, "Struct<'country':String,'id':Uint64,'person':Struct<'is_ok':Bool,'name':String>>");
        }
    }

    void TestTupleTypeFormat() {
        auto t = FunctionTypeInfoBuilder.Tuple(2)
            ->Add<bool>()
            .Add<char*>()
            .Build();
        {
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Tuple<Bool,String>");
        }
    }

    void TestVariantTypeFormat() {
        {
            ui32 index = 0;
            auto t = FunctionTypeInfoBuilder.Struct(2)
                ->AddField<bool>("is_ok", &index)
                .AddField<char*>("name", &index)
                .Build();

            auto s = FormatType(FunctionTypeInfoBuilder.Variant()->Over(t).Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Variant<'is_ok':Bool,'name':String>");
        }
        {
            auto t = FunctionTypeInfoBuilder.Tuple(2)
                ->Add<bool>()
                .Add<char*>()
                .Build();

            auto s = FormatType(FunctionTypeInfoBuilder.Variant()->Over(t).Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Variant<Bool,String>");
        }
    }

    void TestCallableTypeFormat() {
        {
            auto t = FunctionTypeInfoBuilder.Callable(0)->Returns<char*>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<()->String>");
        }
        {
            auto t = FunctionTypeInfoBuilder.Callable(1)->Arg<i64>().Returns<char*>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64)->String>");
        }
        {
            auto t = FunctionTypeInfoBuilder.Callable(2)->Arg<i64>().Arg<char*>().Returns<bool>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64,String)->Bool>");
        }
        {
            auto arg3 = FunctionTypeInfoBuilder.Optional()->Item<char*>().Build();
            auto t = FunctionTypeInfoBuilder.Callable(3)->Arg<i64>().Arg<NUdf::TUtf8>().Arg(arg3).OptionalArgs(1).Returns<bool>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64,Utf8,[String?])->Bool>");
        }
    }

    void TestDataTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<i8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<i16>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int16");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<i32>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int32");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<i64>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int64");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<ui8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<ui16>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint16");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<ui32>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint32");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<ui64>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint64");
        }

        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<float>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Float");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<double>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Double");
        }

        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<char*>());
            UNIT_ASSERT_VALUES_EQUAL(s, "String");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TUtf8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Utf8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TYson>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Yson");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TJson>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Json");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TUuid>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uuid");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TJsonDocument>());
            UNIT_ASSERT_VALUES_EQUAL(s, "JsonDocument");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TDate>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Date");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TDatetime>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Datetime");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TTimestamp>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Timestamp");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TInterval>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Interval");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TTzDate>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzDate");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TTzDatetime>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzDatetime");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TTzTimestamp>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzTimestamp");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.SimpleType<NUdf::TDyNumber>());
            UNIT_ASSERT_VALUES_EQUAL(s, "DyNumber");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder.Decimal(7, 3));
            UNIT_ASSERT_VALUES_EQUAL(s, "Decimal(7,3)");
        }
    }

    void TestArrowType() {
        auto type = FunctionTypeInfoBuilder.SimpleType<ui64>();
        auto atype1 = TypeInfoHelper->MakeArrowType(type);
        UNIT_ASSERT(atype1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype1.Get())->GetType()->ToString(), std::string("uint64"));
        ArrowSchema s;
        atype1->Export(&s);
        auto atype2 = TypeInfoHelper->ImportArrowType(&s);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype2.Get())->GetType()->ToString(), std::string("uint64"));
    }
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLTypeBuilderTest);

}
}
