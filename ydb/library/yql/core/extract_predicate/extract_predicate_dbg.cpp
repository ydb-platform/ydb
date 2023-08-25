#include "extract_predicate_dbg.h"
#include "extract_predicate.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {
namespace {

TExprNode::TPtr ExpandRangeComputeFor(const TExprNode::TPtr& node, TExprContext& ctx,
    const TIntrusivePtr<TTypeAnnotationContext>& typesCtx)
{
    if (!node->IsCallable("RangeComputeFor")) {
        return node;
    }

    YQL_CLOG(DEBUG, Core) << "Expanding " << node->Content();

    TPositionHandle pos = node->Pos();
    TExprNode::TPtr result = ctx.NewCallable(pos, "Nothing", { ExpandType(pos, *node->GetTypeAnn(), ctx) });

    TPredicateExtractorSettings settings;
    settings.HaveNextValueCallable = true;
    auto extractor = MakePredicateRangeExtractor(settings);
    YQL_ENSURE(extractor);

    TVector<TString> indexKeys;
    for (auto& key : node->Tail().ChildrenList()) {
        YQL_ENSURE(key->IsAtom());
        indexKeys.push_back(ToString(key->Content()));
    }

    THashSet<TString> possibleKeys;
    if (!extractor->Prepare(node->ChildPtr(1), *node->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType(), possibleKeys, ctx, *typesCtx)) {
        YQL_CLOG(DEBUG, Core) << "Prepare: ranges can not be built for predicate";
        return result;
    }

    if (AllOf(indexKeys, [&possibleKeys](const TString& key) { return !possibleKeys.contains(key); })) {
        YQL_CLOG(DEBUG, Core) << "No intersection between possible range keys and actual keys";
        return result;
    }


    auto buildResult = extractor->BuildComputeNode(indexKeys, ctx, *typesCtx);
    if (!buildResult.ComputeNode) {
        YQL_CLOG(DEBUG, Core) << "BuildComputeNode: ranges can not be built for predicate";
        return result;
    }

    TString prunedLambdaSerialized;
    {
        auto ast = ConvertToAst(*buildResult.PrunedLambda, ctx, TExprAnnotationFlags::None, true);
        YQL_ENSURE(ast.Root);
        prunedLambdaSerialized = "__yql_ast:" + ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    }

    result = ctx.Builder(pos)
        .Callable("Just")
            .List(0)
                .Add(0, buildResult.ComputeNode)
                .Callable(1, "AsTagged")
                    .Callable(0, "String")
                        .Atom(0, prunedLambdaSerialized)
                    .Seal()
                    .Atom(1, "AST", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return result;
}

} // namespace

THolder<IGraphTransformer> MakeExpandRangeComputeForTransformer(const TIntrusivePtr<TTypeAnnotationContext>& types) {
    return CreateFunctorTransformer(
        [types](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            TOptimizeExprSettings settings(types.Get());
            settings.CustomInstantTypeTransformer = types->CustomInstantTypeTransformer.Get();
            using namespace std::placeholders;
            return OptimizeExpr(input, output, std::bind(&ExpandRangeComputeFor, _1, _2, types), ctx, settings);
        }
    );
}

} // namespace NYql
