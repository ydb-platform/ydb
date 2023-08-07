#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/public/purecalc/purecalc.h>
#include <ydb/library/yql/public/purecalc/io_specs/protobuf/spec.h>
#include <ydb/library/yql/public/purecalc/ut/protos/test_structs.pb.h>
#include <ydb/library/yql/public/udf/udf_counter.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <library/cpp/testing/unittest/registar.h>

class TMyModule : public NKikimr::NUdf::IUdfModule {
public:
    class TFunc : public NKikimr::NUdf::TBoxedValue {
    public:
        TFunc(NKikimr::NUdf::TCounter counter, NKikimr::NUdf::TScopedProbe scopedProbe)
            : Counter_(counter)
            , ScopedProbe_(scopedProbe)
        {}

        NKikimr::NUdf::TUnboxedValue Run(const NKikimr::NUdf::IValueBuilder* valueBuilder, const NKikimr::NUdf::TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            with_lock(ScopedProbe_) {
                Counter_.Inc();
                return NKikimr::NUdf::TUnboxedValuePod(args[0].Get<i32>());
            }
        }

    private:
        mutable NKikimr::NUdf::TCounter Counter_;
        mutable NKikimr::NUdf::TScopedProbe ScopedProbe_;
    };

    void GetAllFunctions(NKikimr::NUdf::IFunctionsSink& sink) const override {
        Y_UNUSED(sink);
    }

    void BuildFunctionTypeInfo(
        const NKikimr::NUdf::TStringRef& name,
        NKikimr::NUdf::TType* userType,
        const NKikimr::NUdf::TStringRef& typeConfig,
        ui32 flags,
        NKikimr::NUdf::IFunctionTypeInfoBuilder& builder) const override {
        Y_UNUSED(userType);
        Y_UNUSED(typeConfig);
        Y_UNUSED(flags);
        if (name == NKikimr::NUdf::TStringRef::Of("Func")) {
            builder.SimpleSignature<i32(i32)>();
            builder.Implementation(new TFunc(
                builder.GetCounter("FuncCalls",true),
                builder.GetScopedProbe("FuncTime")
            ));
        }
    }

    void CleanupOnTerminate() const override {
    }
};

class TMyCountersProvider : public NKikimr::NUdf::ICountersProvider, public NKikimr::NUdf::IScopedProbeHost {
public:
    TMyCountersProvider(i64* calls, TString* log)
        : Calls_(calls)
        , Log_(log)
    {}

    NKikimr::NUdf::TCounter GetCounter(const NKikimr::NUdf::TStringRef& module, const NKikimr::NUdf::TStringRef& name, bool deriv) override {
        UNIT_ASSERT_VALUES_EQUAL(module, "MyModule");
        UNIT_ASSERT_VALUES_EQUAL(name, "FuncCalls");
        UNIT_ASSERT_VALUES_EQUAL(deriv, true);
        return NKikimr::NUdf::TCounter(Calls_);
    }

    NKikimr::NUdf::TScopedProbe GetScopedProbe(const NKikimr::NUdf::TStringRef& module, const NKikimr::NUdf::TStringRef& name) override {
        UNIT_ASSERT_VALUES_EQUAL(module, "MyModule");
        UNIT_ASSERT_VALUES_EQUAL(name, "FuncTime");
        return NKikimr::NUdf::TScopedProbe(Log_ ? this : nullptr, Log_);
    }

    void Acquire(void* cookie) override {
        UNIT_ASSERT(cookie == Log_);
        *Log_ += "Enter\n";
    }

    void Release(void* cookie) override {
        UNIT_ASSERT(cookie == Log_);
        *Log_ += "Exit\n";
    }

private:
    i64* Calls_;
    TString* Log_;
};

namespace NPureCalcProto {
    class TUnparsed;
    class TParsed;
}

class TDocInput : public NYql::NPureCalc::IStream<NPureCalcProto::TUnparsed*> {
public:
    NPureCalcProto::TUnparsed* Fetch() override {
        if (Extracted) {
            return nullptr;
        }

        Extracted = true;
        Msg.SetS("foo");
        return &Msg;
    }

public:
    NPureCalcProto::TUnparsed Msg;
    bool Extracted = false;
};

Y_UNIT_TEST_SUITE(TestUdf) {
    Y_UNIT_TEST(TestCounters) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        i64 callCounter = 0;
        TMyCountersProvider myCountersProvider(&callCounter, nullptr);
        factory->AddUdfModule("MyModule", new TMyModule);
        factory->SetCountersProvider(&myCountersProvider);

        auto program = factory->MakePullStreamProgram(
            TProtobufInputSpec<NPureCalcProto::TUnparsed>(),
            TProtobufOutputSpec<NPureCalcProto::TParsed>(),
            "select MyModule::Func(1) as A, 2 as B, 3 as C from Input",
            ETranslationMode::SQL);

        auto out = program->Apply(MakeHolder<TDocInput>());
        auto* message = out->Fetch();
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->GetA(), 1);
        UNIT_ASSERT_VALUES_EQUAL(message->GetB(), 2);
        UNIT_ASSERT_VALUES_EQUAL(message->GetC(), 3);
        UNIT_ASSERT_VALUES_EQUAL(callCounter, 1);
        UNIT_ASSERT(!out->Fetch());
    }

    Y_UNIT_TEST(TestCountersFilteredColumns) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        i64 callCounter = 0;
        TMyCountersProvider myCountersProvider(&callCounter, nullptr);
        factory->AddUdfModule("MyModule", new TMyModule);
        factory->SetCountersProvider(&myCountersProvider);

        auto ospec = TProtobufOutputSpec<NPureCalcProto::TParsed>();
        ospec.SetOutputColumnsFilter(THashSet<TString>({"B", "C"}));
        auto program = factory->MakePullStreamProgram(
            TProtobufInputSpec<NPureCalcProto::TUnparsed>(),
            ospec,
            "select MyModule::Func(1) as A, 2 as B, 3 as C from Input",
            ETranslationMode::SQL);

        auto out = program->Apply(MakeHolder<TDocInput>());
        auto* message = out->Fetch();
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->GetA(), 0);
        UNIT_ASSERT_VALUES_EQUAL(message->GetB(), 2);
        UNIT_ASSERT_VALUES_EQUAL(message->GetC(), 3);
        UNIT_ASSERT_VALUES_EQUAL(callCounter, 0);
        UNIT_ASSERT(!out->Fetch());
    }

    Y_UNIT_TEST(TestScopedProbes) {
        using namespace NYql::NPureCalc;

        auto factory = MakeProgramFactory();

        TString log;
        TMyCountersProvider myCountersProvider(nullptr, &log);
        factory->AddUdfModule("MyModule", new TMyModule);
        factory->SetCountersProvider(&myCountersProvider);

        auto program = factory->MakePullStreamProgram(
            TProtobufInputSpec<NPureCalcProto::TUnparsed>(),
            TProtobufOutputSpec<NPureCalcProto::TParsed>(),
            "select MyModule::Func(1) as A, 2 as B, 3 as C from Input",
            ETranslationMode::SQL);

        auto out = program->Apply(MakeHolder<TDocInput>());
        auto* message = out->Fetch();
        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(message->GetA(), 1);
        UNIT_ASSERT_VALUES_EQUAL(message->GetB(), 2);
        UNIT_ASSERT_VALUES_EQUAL(message->GetC(), 3);
        UNIT_ASSERT_VALUES_EQUAL(log, "Enter\nExit\n");
        UNIT_ASSERT(!out->Fetch());
    }
}
