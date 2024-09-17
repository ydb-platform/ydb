#include "lambda_builder.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_opt_literal.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <util/generic/strbuf.h>
#include <util/system/env.h>

namespace NYql {

using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TLambdaBuilder::TLambdaBuilder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        NKikimr::NMiniKQL::TScopedAlloc& alloc,
        const NKikimr::NMiniKQL::TTypeEnvironment* env,
        const TIntrusivePtr<IRandomProvider>& randomProvider,
        const TIntrusivePtr<ITimeProvider>& timeProvider,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats,
        NKikimr::NUdf::ICountersProvider* counters,
        const NKikimr::NUdf::ISecureParamsProvider* secureParamsProvider)
    : FunctionRegistry(functionRegistry)
    , Alloc(alloc)
    , RandomProvider(randomProvider)
    , TimeProvider(timeProvider)
    , JobStats(jobStats)
    , Counters(counters)
    , SecureParamsProvider(secureParamsProvider)
    , Env(env)
{
}

TLambdaBuilder::~TLambdaBuilder() {
}

void TLambdaBuilder::SetExternalEnv(const NKikimr::NMiniKQL::TTypeEnvironment* env) {
    Env = env;
}

const NKikimr::NMiniKQL::TTypeEnvironment* TLambdaBuilder::CreateTypeEnv() const {
    YQL_ENSURE(!EnvPtr);
    EnvPtr = std::make_shared<NKikimr::NMiniKQL::TTypeEnvironment>(Alloc);
    return EnvPtr.get();
}

TRuntimeNode TLambdaBuilder::BuildLambda(const IMkqlCallableCompiler& compiler, const TExprNode::TPtr& lambdaNode,
    TExprContext& exprCtx, TArgumentsMap&& arguments) const
{
    TProgramBuilder pgmBuilder(GetTypeEnvironment(), *FunctionRegistry);
    TMkqlBuildContext ctx(compiler, pgmBuilder, exprCtx, lambdaNode->UniqueId(), std::move(arguments));
    return MkqlBuildExpr(*lambdaNode, ctx);
}

TRuntimeNode TLambdaBuilder::TransformAndOptimizeProgram(NKikimr::NMiniKQL::TRuntimeNode root,
    TCallableVisitFuncProvider funcProvider) {
    TExploringNodeVisitor explorer;
    explorer.Walk(root.GetNode(), GetTypeEnvironment());
    bool wereChanges = false;
    TRuntimeNode program = SinglePassVisitCallables(root, explorer, funcProvider, GetTypeEnvironment(), true, wereChanges);
    program = LiteralPropagationOptimization(program, GetTypeEnvironment(), true);
    return program;
}

THolder<IComputationGraph> TLambdaBuilder::BuildGraph(
    const NKikimr::NMiniKQL::TComputationNodeFactory& factory,
    NUdf::EValidateMode validateMode,
    NUdf::EValidatePolicy validatePolicy,
    const TString& optLLVM,
    NKikimr::NMiniKQL::EGraphPerProcess graphPerProcess,
    TExploringNodeVisitor& explorer,
    TRuntimeNode root) const
{
    return BuildGraph(factory, validateMode, validatePolicy, optLLVM, graphPerProcess, explorer, root, {root.GetNode()});
}

std::tuple<
    THolder<NKikimr::NMiniKQL::IComputationGraph>,
    TIntrusivePtr<IRandomProvider>,
    TIntrusivePtr<ITimeProvider>
>
TLambdaBuilder::BuildLocalGraph(
    const NKikimr::NMiniKQL::TComputationNodeFactory& factory,
    NUdf::EValidateMode validateMode,
    NUdf::EValidatePolicy validatePolicy,
    const TString& optLLVM,
    NKikimr::NMiniKQL::EGraphPerProcess graphPerProcess,
    TExploringNodeVisitor& explorer,
    TRuntimeNode root) const
{
    auto randomProvider = RandomProvider;
    auto timeProvider = TimeProvider;
    if (GetEnv(TString("YQL_DETERMINISTIC_MODE"))) {
        randomProvider = CreateDeterministicRandomProvider(1);
        timeProvider = CreateDeterministicTimeProvider(10000000);
    }

    return std::make_tuple(BuildGraph(factory, validateMode, validatePolicy, optLLVM, graphPerProcess, explorer, root, {root.GetNode()},
        randomProvider, timeProvider), randomProvider, timeProvider);
}

class TComputationGraphProxy: public IComputationGraph {
public:
    TComputationGraphProxy(IComputationPattern::TPtr&& pattern, THolder<IComputationGraph>&& graph)
        : Pattern(std::move(pattern))
        , Graph(std::move(graph))
    {}

    void Prepare() final {
        Graph->Prepare();
    }
    NUdf::TUnboxedValue GetValue() final {
        return Graph->GetValue();
    }
    TComputationContext& GetContext() final {
        return Graph->GetContext();
    }
    IComputationExternalNode* GetEntryPoint(size_t index, bool require) override {
        return Graph->GetEntryPoint(index, require);
    }
    const TArrowKernelsTopology* GetKernelsTopology() override {
        return Graph->GetKernelsTopology();
    }
    const TComputationNodePtrDeque& GetNodes() const final {
        return Graph->GetNodes();
    }
    void Invalidate() final {
        return Graph->Invalidate();
    }
    TMemoryUsageInfo& GetMemInfo() const final {
        return Graph->GetMemInfo();
    }
    const THolderFactory& GetHolderFactory() const final {
        return Graph->GetHolderFactory();
    }
    ITerminator* GetTerminator() const final {
        return Graph->GetTerminator();
    }
    bool SetExecuteLLVM(bool value) final {
        return Graph->SetExecuteLLVM(value);
    }
    TString SaveGraphState() final {
        return Graph->SaveGraphState();
    }
    void LoadGraphState(TStringBuf state) final {
        Graph->LoadGraphState(state);
    }
private:
    IComputationPattern::TPtr Pattern;
    THolder<IComputationGraph> Graph;
};


THolder<IComputationGraph> TLambdaBuilder::BuildGraph(
    const NKikimr::NMiniKQL::TComputationNodeFactory& factory,
    NUdf::EValidateMode validateMode,
    NUdf::EValidatePolicy validatePolicy,
    const TString& optLLVM,
    NKikimr::NMiniKQL::EGraphPerProcess graphPerProcess,
    TExploringNodeVisitor& explorer,
    TRuntimeNode& root,
    std::vector<NKikimr::NMiniKQL::TNode*>&& entryPoints,
    TIntrusivePtr<IRandomProvider> randomProvider,
    TIntrusivePtr<ITimeProvider> timeProvider) const
{
    if (!randomProvider) {
        randomProvider = RandomProvider;
    }

    if (!timeProvider) {
        timeProvider = TimeProvider;
    }

    TString serialized;

    TComputationPatternOpts patternOpts(Alloc.Ref(), GetTypeEnvironment());
    patternOpts.SetOptions(factory, FunctionRegistry, validateMode, validatePolicy, optLLVM, graphPerProcess, JobStats, Counters, SecureParamsProvider);
    auto preparePatternFunc = [&]() {
        if (serialized) {
            auto tupleRunTimeNodes = DeserializeRuntimeNode(serialized, GetTypeEnvironment());
            auto tupleNodes = static_cast<const TTupleLiteral*>(tupleRunTimeNodes.GetNode());
            root = tupleNodes->GetValue(0);
            for (size_t index = 0; index < entryPoints.size(); ++index) {
                entryPoints[index] = tupleNodes->GetValue(1 + index).GetNode();
            }
        }
        explorer.Walk(root.GetNode(), GetTypeEnvironment());
        auto pattern = MakeComputationPattern(explorer, root, entryPoints, patternOpts);
        for (const auto& node : explorer.GetNodes()) {
            node->SetCookie(0);
        }
        return pattern;
    };

    auto pattern = preparePatternFunc();
    YQL_ENSURE(pattern);

    const TComputationOptsFull computeOpts(JobStats, Alloc.Ref(), GetTypeEnvironment(), *randomProvider, *timeProvider, validatePolicy, SecureParamsProvider, Counters);
    auto graph = pattern->Clone(computeOpts);
    return MakeHolder<TComputationGraphProxy>(std::move(pattern), std::move(graph));
}

TRuntimeNode TLambdaBuilder::MakeTuple(const TVector<TRuntimeNode>& items) const {
    TProgramBuilder pgmBuilder(GetTypeEnvironment(), *FunctionRegistry);
    return pgmBuilder.NewTuple(items);
}

TRuntimeNode TLambdaBuilder::Deserialize(const TString& code) {
    return DeserializeRuntimeNode(code, GetTypeEnvironment());
}

std::pair<TString, size_t> TLambdaBuilder::Serialize(TRuntimeNode rootNode) {
    TExploringNodeVisitor explorer;
    explorer.Walk(rootNode.GetNode(), GetTypeEnvironment());
    TString code = SerializeRuntimeNode(explorer, rootNode, GetTypeEnvironment());
    size_t nodes = explorer.GetNodes().size();
    return std::make_pair(code, nodes);
}

TRuntimeNode TLambdaBuilder::UpdateLambdaCode(TString& code, size_t& nodes, TCallableVisitFuncProvider funcProvider) {
    TRuntimeNode rootNode = Deserialize(code);
    rootNode = TransformAndOptimizeProgram(rootNode, funcProvider);
    std::tie(code, nodes) = Serialize(rootNode);
    return rootNode;
}

}
