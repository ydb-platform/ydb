#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>

using namespace NYql::NPureCalc;

namespace {

// TODO(YQL-20095): Explore real problem to fix this.
// NOLINTNEXTLINE(bugprone-exception-escape)
class TStatelessInputSpec: public TInputSpecBase {
public:
    TStatelessInputSpec()
        : Schemas_({NYT::TNode::CreateList()
                        .Add("StructType")
                        .Add(NYT::TNode::CreateList()
                                 .Add(NYT::TNode::CreateList()
                                          .Add("InputValue")
                                          .Add(NYT::TNode::CreateList()
                                                   .Add("DataType")
                                                   .Add("Utf8"))))})
    {};

    const TVector<NYT::TNode>& GetSchemas() const override {
        return Schemas_;
    }

private:
    const TVector<NYT::TNode> Schemas_;
};

class TStatelessInputConsumer: public IConsumer<const NYql::NUdf::TUnboxedValue&> {
public:
    explicit TStatelessInputConsumer(TWorkerHolder<IPushStreamWorker> worker)
        : Worker_(std::move(worker))
    {
    }

    void OnObject(const NYql::NUdf::TUnboxedValue& value) override {
        with_lock (Worker_->GetScopedAlloc()) {
            NYql::NUdf::TUnboxedValue* items = nullptr;
            NYql::NUdf::TUnboxedValue result = Worker_->GetGraph().GetHolderFactory().CreateDirectArrayHolder(1, items);

            items[0] = value;

            Worker_->Push(std::move(result));

            // Clear graph after each object because
            // values allocated on another allocator and should be released
            Worker_->Invalidate();
        }
    }

    void OnFinish() override {
        with_lock (Worker_->GetScopedAlloc()) {
            Worker_->OnFinish();
        }
    }

private:
    TWorkerHolder<IPushStreamWorker> Worker_;
};

class TStatelessConsumer: public IConsumer<NPureCalcProto::TStringMessage*> {
    const TString ExpectedData_;
    const ui64 ExpectedRows_;
    ui64 RowId_ = 0;

public:
    TStatelessConsumer(const TString& expectedData, ui64 expectedRows)
        : ExpectedData_(expectedData)
        , ExpectedRows_(expectedRows)
    {
    }

    void OnObject(NPureCalcProto::TStringMessage* message) override {
        UNIT_ASSERT_VALUES_EQUAL_C(ExpectedData_, message->GetX(), RowId_);
        RowId_++;
    }

    void OnFinish() override {
        UNIT_ASSERT_VALUES_EQUAL(ExpectedRows_, RowId_);
    }
};
} // namespace

template <>
struct TInputSpecTraits<TStatelessInputSpec> {
    static constexpr bool IsPartial = false;
    static constexpr bool SupportPushStreamMode = true;

    using TConsumerType = THolder<IConsumer<const NYql::NUdf::TUnboxedValue&>>;

    static TConsumerType MakeConsumer(const TStatelessInputSpec&, TWorkerHolder<IPushStreamWorker> worker) {
        return MakeHolder<TStatelessInputConsumer>(std::move(worker));
    }
};

Y_UNIT_TEST_SUITE(TestMixedAllocators) {
Y_UNIT_TEST(TestPushStream) {
    const auto targetString = "large string >= 14 bytes";
    const auto factory = MakeProgramFactory();
    const auto sql = TStringBuilder() << "SELECT InputValue AS X FROM Input WHERE InputValue = \"" << targetString << "\";";

    const auto program = factory->MakePushStreamProgram(
        TStatelessInputSpec(),
        TProtobufOutputSpec<NPureCalcProto::TStringMessage>(),
        sql);

    const ui64 numberRows = 5;
    const auto inputConsumer = program->Apply(MakeHolder<TStatelessConsumer>(targetString, numberRows));
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);

    const auto pushString = [&](TString inputValue) {
        NYql::NUdf::TUnboxedValue stringValue;
        with_lock (alloc) {
            stringValue = NKikimr::NMiniKQL::MakeString(inputValue);
            alloc.Ref().LockObject(stringValue);
        }

        inputConsumer->OnObject(stringValue);

        with_lock (alloc) {
            alloc.Ref().UnlockObject(stringValue);
            stringValue.Clear();
        }
    };

    for (ui64 i = 0; i < numberRows; ++i) {
        pushString(targetString);
        pushString("another large string >= 14 bytes");
    }
    inputConsumer->OnFinish();
}
} // Y_UNIT_TEST_SUITE(TestMixedAllocators)
