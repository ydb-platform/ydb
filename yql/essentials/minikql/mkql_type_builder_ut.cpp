#include "mkql_type_builder.h"

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/c/abi.h>

namespace NKikimr::NMiniKQL {

class TMiniKQLTypeBuilderTest: public TTestBase {
public:
    TMiniKQLTypeBuilderTest()
        : Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , TypeInfoHelper_(new TTypeInfoHelper())
        , FunctionTypeInfoBuilder_(NYql::UnknownLangVersion, Env_, TypeInfoHelper_, "", nullptr, NYql::NUdf::TSourcePosition())
    {
    }

private:
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper_;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder_;

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
    UNIT_TEST(TestArrowTaggedType);
    UNIT_TEST_SUITE_END();

    TString FormatType(NUdf::TType* t) {
        TStringBuilder output;
        NUdf::TTypePrinter p(*TypeInfoHelper_, t);
        p.Out(output.Out);
        return output;
    }

    void TestPgTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Pg(NYql::NPg::LookupType("bool").TypeId));
            UNIT_ASSERT_VALUES_EQUAL(s, "pgbool");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Pg(NYql::NPg::LookupType("_bool").TypeId));
            UNIT_ASSERT_VALUES_EQUAL(s, "_pgbool");
        }
    }

    void TestBlockTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Block(true)->Item<ui32>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Scalar<Uint32>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Block(false)->Item<ui32>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Block<Uint32>");
        }
    }

    void TestSingularTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Null());
            UNIT_ASSERT_VALUES_EQUAL(s, "Null");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Void());
            UNIT_ASSERT_VALUES_EQUAL(s, "Void");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.EmptyList());
            UNIT_ASSERT_VALUES_EQUAL(s, "EmptyList");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.EmptyDict());
            UNIT_ASSERT_VALUES_EQUAL(s, "EmptyDict");
        }
    }

    void TestOptionalTypeFormat() {
        auto optional1 = FunctionTypeInfoBuilder_.Optional()->Item<i64>().Build();
        auto optional2 = FunctionTypeInfoBuilder_.Optional()->Item(optional1).Build();
        auto optional3 = FunctionTypeInfoBuilder_.Optional()->Item(optional2).Build();
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
            auto s = FormatType(FunctionTypeInfoBuilder_.List()->Item<i8>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "List<Int8>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Dict()->Key<i8>().Value<bool>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Dict<Int8,Bool>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Set()->Key<char*>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Set<String>");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Stream()->Item<ui64>().Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Stream<Uint64>");
        }
    }

    void TestEnumTypeFormat() {
        ui32 index = 0;
        auto t = FunctionTypeInfoBuilder_.Enum(2)
                     ->AddField("RED", &index)
                     .AddField("GREEN", &index)
                     .AddField("BLUE", &index)
                     .Build();

        auto s = FormatType(t);
        UNIT_ASSERT_VALUES_EQUAL(s, "Enum<'BLUE','GREEN','RED'>");
    }

    void TestResourceTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Resource("my_resource"));
            UNIT_ASSERT_VALUES_EQUAL(s, "Resource<'my_resource'>");
        }
    }

    void TestTaggedTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Tagged(FunctionTypeInfoBuilder_.SimpleType<i8>(), "my_tag"));
            UNIT_ASSERT_VALUES_EQUAL(s, "Tagged<Int8,'my_tag'>");
        }
    }

    void TestStructTypeFormat() {
        ui32 index = 0;
        auto s1 = FunctionTypeInfoBuilder_.Struct(2)
                      ->AddField<bool>("is_ok", &index)
                      .AddField<char*>("name", &index)
                      .Build();

        auto s2 = FunctionTypeInfoBuilder_.Struct(3)
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
        auto t = FunctionTypeInfoBuilder_.Tuple(2)
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
            auto t = FunctionTypeInfoBuilder_.Struct(2)
                         ->AddField<bool>("is_ok", &index)
                         .AddField<char*>("name", &index)
                         .Build();

            auto s = FormatType(FunctionTypeInfoBuilder_.Variant()->Over(t).Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Variant<'is_ok':Bool,'name':String>");
        }
        {
            auto t = FunctionTypeInfoBuilder_.Tuple(2)
                         ->Add<bool>()
                         .Add<char*>()
                         .Build();

            auto s = FormatType(FunctionTypeInfoBuilder_.Variant()->Over(t).Build());
            UNIT_ASSERT_VALUES_EQUAL(s, "Variant<Bool,String>");
        }
    }

    void TestCallableTypeFormat() {
        {
            auto t = FunctionTypeInfoBuilder_.Callable(0)->Returns<char*>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<()->String>");
        }
        {
            auto t = FunctionTypeInfoBuilder_.Callable(1)->Arg<i64>().Returns<char*>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64)->String>");
        }
        {
            auto t = FunctionTypeInfoBuilder_.Callable(2)->Arg<i64>().Arg<char*>().Returns<bool>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64,String)->Bool>");
        }
        {
            auto arg3 = FunctionTypeInfoBuilder_.Optional()->Item<char*>().Build();
            auto t = FunctionTypeInfoBuilder_.Callable(3)->Arg<i64>().Arg<NUdf::TUtf8>().Arg(arg3).OptionalArgs(1).Returns<bool>().Build();
            auto s = FormatType(t);
            UNIT_ASSERT_VALUES_EQUAL(s, "Callable<(Int64,Utf8,[String?])->Bool>");
        }
    }

    void TestDataTypeFormat() {
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<i8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<i16>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int16");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<i32>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int32");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<i64>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Int64");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<ui8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<ui16>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint16");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<ui32>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint32");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<ui64>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uint64");
        }

        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<float>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Float");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<double>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Double");
        }

        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<char*>());
            UNIT_ASSERT_VALUES_EQUAL(s, "String");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TUtf8>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Utf8");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TYson>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Yson");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TJson>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Json");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TUuid>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Uuid");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TJsonDocument>());
            UNIT_ASSERT_VALUES_EQUAL(s, "JsonDocument");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TDate>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Date");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TDatetime>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Datetime");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TTimestamp>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Timestamp");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TInterval>());
            UNIT_ASSERT_VALUES_EQUAL(s, "Interval");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TTzDate>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzDate");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TTzDatetime>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzDatetime");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TTzTimestamp>());
            UNIT_ASSERT_VALUES_EQUAL(s, "TzTimestamp");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.SimpleType<NUdf::TDyNumber>());
            UNIT_ASSERT_VALUES_EQUAL(s, "DyNumber");
        }
        {
            auto s = FormatType(FunctionTypeInfoBuilder_.Decimal(7, 3));
            UNIT_ASSERT_VALUES_EQUAL(s, "Decimal(7,3)");
        }
    }

    void TestArrowType() {
        auto type = FunctionTypeInfoBuilder_.SimpleType<ui64>();
        auto atype1 = TypeInfoHelper_->MakeArrowType(type);
        UNIT_ASSERT(atype1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype1.Get())->GetType()->ToString(), std::string("uint64"));
        ArrowSchema s;
        atype1->Export(&s);
        auto atype2 = TypeInfoHelper_->ImportArrowType(&s);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype2.Get())->GetType()->ToString(), std::string("uint64"));
    }

    void TestArrowTaggedType() {
        auto type = FunctionTypeInfoBuilder_.Tagged(FunctionTypeInfoBuilder_.SimpleType<ui64>(), "my_tag");
        auto atype1 = TypeInfoHelper_->MakeArrowType(type);
        UNIT_ASSERT(atype1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype1.Get())->GetType()->ToString(), std::string("uint64"));
        ArrowSchema s;
        atype1->Export(&s);
        auto atype2 = TypeInfoHelper_->ImportArrowType(&s);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<TArrowType*>(atype2.Get())->GetType()->ToString(), std::string("uint64"));
    }
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLTypeBuilderTest);

Y_UNIT_TEST_SUITE(TLogProviderTest) {
struct TLogMessage {
    TString Component;
    NUdf::ELogLevel Level;
    TString Message;
};

struct TLogProviderSetup {
    explicit TLogProviderSetup(bool withoutLog = false)
        : Alloc(__LOCATION__)
        , Env(Alloc)
        , TypeInfoHelper(new TTypeInfoHelper())
        , LogProvider(NUdf::MakeLogProvider(
              [this](const NUdf::TStringRef& component, NUdf::ELogLevel level, const NUdf::TStringRef& message) {
                  Messages.push_back({TString(component), level, TString(message)});
              }))
        , FunctionTypeInfoBuilder(NYql::UnknownLangVersion, Env, TypeInfoHelper, "module", nullptr, NYql::NUdf::TSourcePosition(), nullptr, withoutLog ? nullptr : LogProvider.Get())
    {
    }

    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    NUdf::ITypeInfoHelper::TPtr TypeInfoHelper;
    TVector<TLogMessage> Messages;
    NUdf::TUniquePtr<NUdf::ILogProvider> LogProvider;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder;
};

Y_UNIT_TEST(WithoutProvider) {
    TLogProviderSetup setup(true);
    auto logger = setup.FunctionTypeInfoBuilder.MakeLogger(true);
    auto comp = logger->RegisterComponent("foo");
    logger->Log(comp, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 0);
}

Y_UNIT_TEST(Simple) {
    TLogProviderSetup setup;
    auto logger = setup.FunctionTypeInfoBuilder.MakeLogger(true);
    auto comp = logger->RegisterComponent("foo");
    logger->Log(comp, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages[0].Component, "module.foo");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages[0].Level, NUdf::ELogLevel::Trace);
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages[0].Message, "hello");
}

Y_UNIT_TEST(WrongComponent) {
    TLogProviderSetup setup;
    auto logger = setup.FunctionTypeInfoBuilder.MakeLogger(true);
    logger->Log(0, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 0);
}

Y_UNIT_TEST(DisabledByDefaultLevel) {
    TLogProviderSetup setup;
    auto logger = setup.FunctionTypeInfoBuilder.MakeLogger(true);
    auto comp = logger->RegisterComponent("foo");
    logger->SetDefaultLevel(NUdf::ELogLevel::Debug);
    logger->Log(comp, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 0);
    logger->Log(comp, NUdf::ELogLevel::Debug, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 1);
}

Y_UNIT_TEST(DisabledByComponentLevel) {
    TLogProviderSetup setup;
    auto logger = setup.FunctionTypeInfoBuilder.MakeLogger(true);
    auto comp1 = logger->RegisterComponent("foo");
    auto comp2 = logger->RegisterComponent("bar");
    logger->SetComponentLevel(comp1, NUdf::ELogLevel::Info);
    logger->SetComponentLevel(comp2, NUdf::ELogLevel::Debug);
    logger->Log(comp1, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 0);
    logger->Log(comp1, NUdf::ELogLevel::Debug, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 0);
    logger->Log(comp1, NUdf::ELogLevel::Info, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 1);
    logger->Log(comp2, NUdf::ELogLevel::Trace, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 1);
    logger->Log(comp2, NUdf::ELogLevel::Debug, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 2);
    logger->Log(comp2, NUdf::ELogLevel::Info, "hello");
    UNIT_ASSERT_VALUES_EQUAL(setup.Messages.size(), 3);
}
} // Y_UNIT_TEST_SUITE(TLogProviderTest)

} // namespace NKikimr::NMiniKQL
