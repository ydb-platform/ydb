#include "kqp_read_ranges_evaluate.h"
#include "kqp_read_ranges_explain.h"

#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>

#include <util/generic/hash.h>

namespace NKikimr::NKqp::NOpt {
namespace {

using namespace NYql;
using namespace NMiniKQL;

// Only literal constructors and the finite range/list operations used by the extractor. Reject external and
// nondeterministic calls, including parameters, clocks, random functions, UDFs, reads and list generators.
bool IsSupportedCallable(const TExprNode& node) {
    return NUdf::FindDataSlot(node.Content()).Defined() || node.IsCallable({
        "DataType", "PgType", "OptionalType", "TupleType", "ListType", "VoidType",
        "PgConst", "Nothing", "Just", "Void", "AsList", "List", "SafeCast", "StrictCast", "ToString",
        "RangeFor", "RangeEmpty", "AsRange", "RangeUnion", "RangeIntersect", "RangeMultiply", "RangeFinalize", "RangeToPg",
        "If", "IfStrict", "IfPresent", "Map", "FlatMap", "Nth", "Take", "Collect", "Length",
        "<", "<=", ">", ">=", "==", "!=", "And", "Or", "Not", "Exists", "Coalesce", "Ensure"
    });
}

bool FitsInputBudget(const TExprNode& root) {
    TVector<std::pair<const TExprNode*, size_t>> pending{{&root, 0}};
    THashMap<const TExprNode*, size_t> depths;
    size_t bytes = 0;
    size_t edges = 0;
    while (!pending.empty()) {
        const auto [node, depth] = pending.back();
        pending.pop_back();
        if (depth > 64) {
            return false;
        }
        const auto [it, inserted] = depths.emplace(node, depth);
        if (!inserted) {
            if (it->second >= depth) {
                continue;
            }
            // A shared subtree reached through a longer path must also respect the depth limit.
            it->second = depth;
        } else {
            bytes += node->Content().size();
            edges += node->ChildrenSize();
        }
        if (depths.size() > MaxRangeEvaluationNodes || edges > MaxRangeEvaluationNodes || bytes > MaxRangeEvaluationBytes
            || node->Type() == TExprNode::World || (node->IsCallable() && !IsSupportedCallable(*node))) {
            return false;
        }
        for (const auto& child : node->Children()) {
            pending.emplace_back(child.Get(), depth + 1);
        }
    }
    return true;
}

} // namespace

TExprNode::TPtr EvaluateReadRanges(const TExprNode::TPtr& ranges, TMaybe<size_t> expectedMaxRanges,
    TExprContext& ctx, const TTypeAnnotationContext& types, const IFunctionRegistry& functionRegistry) {
    if (!ranges || !expectedMaxRanges || *expectedMaxRanges > MaxEvaluatedRangeInputs) {
        return nullptr;
    }

    try {
        if (!FitsInputBudget(*ranges)) {
            return nullptr;
        }
        TExprContext evalCtx;
        evalCtx.NodesAllocationLimit = 100000;
        evalCtx.StringsAllocationLimit = 1024 * 1024;
        evalCtx.RepeatTransformLimit = 1024;
        TNodeOnNodeOwnedMap clones;
        auto expression = evalCtx.DeepCopy(*ranges, ctx, clones, /*internStrings=*/true, /*copyTypes=*/false);
        auto evalTypes = MakeIntrusive<TTypeAnnotationContext>();
        evalTypes->LangVer = types.LangVer;
        evalTypes->BackportMode = types.BackportMode;
        evalTypes->UpdateDecimalConversionMode(types.GetDecimalConversionMode());
        evalTypes->RuntimeSettings = types.RuntimeSettings;
        TPeepholeSettings settings;
        settings.WithNonDeterministicRules = false;
        bool hasNonDeterministicFunctions = false;
        if (PeepHoleOptimizeNode(expression, expression, evalCtx, *evalTypes, nullptr, hasNonDeterministicFunctions, settings)
                != IGraphTransformer::TStatus::Ok
            || hasNonDeterministicFunctions || !expression->GetTypeAnn()
            || expression->GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
            return nullptr;
        }

        TScopedAlloc alloc(__LOCATION__);
        alloc.SetLimit(16 * 1024 * 1024);
        TTypeEnvironment env(alloc);
        TProgramBuilder builder(env, functionRegistry);
        NCommon::TMkqlCommonCallableCompiler compiler;
        NCommon::TMkqlBuildContext buildCtx(compiler, builder, evalCtx);
        auto program = NCommon::MkqlBuildExpr(*expression, buildCtx);
        // Do not materialize a large literal result merely to discard it in the formatter.
        program = builder.Take(program, builder.NewDataLiteral<ui64>(MaxExplainedReadRanges + 1));
        TExploringNodeVisitor explorer;
        explorer.Walk(program.GetNode(), env);
        TComputationPatternOpts opts(alloc.Ref(), env, GetCompositeWithBuiltinFactory({GetPgFactory()}), &functionRegistry,
            NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi,
            nullptr, nullptr, nullptr, nullptr, types.LangVer, types.RuntimeSettings);
        auto pattern = MakeComputationPattern(explorer, program, {}, opts);
        auto random = CreateDefaultRandomProvider();
        auto time = CreateDefaultTimeProvider();
        auto graph = pattern->Clone(opts.ToComputationOptions(*random, *time));
        TOnlyThrowingBindTerminator terminator;
        const auto value = graph->GetValue();
        if (value.GetListLength() > MaxExplainedReadRanges) {
            return nullptr;
        }
        const auto literal = NCommon::ValueToExprLiteral(expression->GetTypeAnn(), value, evalCtx, expression->Pos());
        clones.clear();
        return ctx.DeepCopy(*literal, evalCtx, clones, /*internStrings=*/true, /*copyTypes=*/false);
    } catch (...) {
        // Explain is optional. No query issues, executor logging or changes to the executable ranges on failure.
        return nullptr;
    }
}

} // namespace NKikimr::NKqp::NOpt
