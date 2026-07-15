#include <yql/essentials/public/purecalc/purecalc.h>
#include <yql/essentials/public/purecalc/common/default_runtime_settings.h>
#include <yql/essentials/public/purecalc/io_specs/protobuf/spec.h>
#include <yql/essentials/public/purecalc/ut/protos/test_structs.pb.h>
#include <yql/essentials/public/purecalc/ut/empty_stream.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

class TWorkerHolderOutputSpec: public NYql::NPureCalc::TOutputSpecBase {
public:
    const NYT::TNode& GetSchema() const override {
        static const NYT::TNode Entity = NYT::TNode::CreateEntity();
        return Entity;
    }
};

} // namespace

template <>
struct NYql::NPureCalc::TOutputSpecTraits<TWorkerHolderOutputSpec> {
    static const constexpr bool IsPartial = false;
    static const constexpr bool SupportPullListMode = true;

    using TPullListReturnType = const NYql::TRuntimeSettings*;

    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const TWorkerHolderOutputSpec&,
        NYql::NPureCalc::TWorkerHolder<NYql::NPureCalc::IPullListWorker> worker)
    {
        return &worker->GetGraph().GetContext().RuntimeSettings;
    }
};

Y_UNIT_TEST_SUITE(TestRuntimeSettings) {

Y_UNIT_TEST(DefaultRuntimeSettings) {
    using namespace NYql::NPureCalc;

    auto factory = MakeProgramFactory();

    auto program = factory->MakePullListProgram(
        TProtobufInputSpec<NPureCalcProto::TStringMessage>(),
        TWorkerHolderOutputSpec(),
        "SELECT X FROM Input",
        ETranslationMode::SQL);

    // The worker must use the default runtime settings singleton, not a custom copy.
    UNIT_ASSERT_EQUAL(program->Apply(EmptyStream<NPureCalcProto::TStringMessage*>()),
                      NYql::NPureCalc::NPrivate::GetDefaultRuntimeSettings().Get());
}

} // Y_UNIT_TEST_SUITE(TestRuntimeSettings)
