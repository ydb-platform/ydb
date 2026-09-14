#include "yql_opt_block_rewriter.h"

#include <yql/essentials/core/yql_default_valid_value.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/utils/log/log.h>

#include <yql/essentials/utils/checkpoint_map.h>

#include <util/generic/scope.h>

namespace NYql {

namespace {

struct TBlockFuncRule {
    std::string_view Name;
};

using TBlockFuncMap = std::unordered_map<std::string_view, TBlockFuncRule>;

struct TBlockRules {

    // all kernels whose name begins with capital letter are YQL kernel
    static constexpr std::initializer_list<TBlockFuncMap::value_type> FuncsInit = {
        {"Abs", { "Abs" } },
        {"Minus", { "Minus" } },

        {"+", { "Add" } },
        {"-", { "Sub" } },
        {"*", { "Mul" } },
        {"/", { "Div" } },
        {"%", { "Mod" } },

        // comparison kernels
        {"==", { "Equals" } },
        {"!=", { "NotEquals" } },
        {"<",  { "Less" } },
        {"<=", { "LessOrEqual" } },
        {">",  { "Greater" } },
        {">=", { "GreaterOrEqual" } },

        // string kernels
        {"Size", { "Size" } },
        {"StartsWith", { "StartsWith" } },
        {"EndsWith", { "EndsWith" } },
        {"StringContains", { "StringContains" } },
    };

    TBlockRules()
        : Funcs(FuncsInit)
    {}

    static const TBlockRules& Instance() {
        return *Singleton<TBlockRules>();
    }

    const TBlockFuncMap Funcs;
};

TExprNode::TPtr SplitByPairs(TPositionHandle pos, const TStringBuf& funcName, const TExprNode::TListType& funcArgs,
    size_t begin, size_t end, TExprContext& ctx)
{
    if (end == begin + 1) {
        return funcArgs[begin];
    }
    YQL_ENSURE(end >= begin + 2);
    const size_t len = end - begin;
    if (len < 4) {
        auto result = ctx.NewCallable(pos, funcName, { funcArgs[begin], funcArgs[begin + 1] });
        if (len == 3) {
            result = ctx.NewCallable(pos, funcName, { result, funcArgs[begin + 2] });
        }
        return result;
    }

    auto left = SplitByPairs(pos, funcName, funcArgs, begin, begin + len / 2, ctx);
    auto right = SplitByPairs(pos, funcName, funcArgs, begin + len / 2, end, ctx);
    return ctx.NewCallable(pos, funcName, { left, right });
}

using TExprNodePtrPred = std::function<bool(const TExprNode::TPtr&)>;

TExprNodePtrPred MakeBlockRewriteStopPredicate(const TExprNode::TPtr& lambda, bool skipAnyLambdaArguments, bool skipAllInternalLambdas) {
    return [lambda, skipAnyLambdaArguments, skipAllInternalLambdas](const TExprNode::TPtr& node) {
        return (node->IsArguments() && skipAnyLambdaArguments) || (node->IsLambda() && node != lambda && skipAllInternalLambdas);
    };
}

void DoMarkLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop, TNodeSet& visited, bool markAll) {
    if (!visited.insert(node.Get()).second) {
        return;
    }

    if (needStop(node)) {
        return;
    }

    if (markAll) {
        lazyNodes.insert(node.Get());
    }

    const bool isLazyNode = node->IsCallable({"And", "Or", "If", "Coalesce"});
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        DoMarkLazy(node->ChildPtr(i), lazyNodes, needStop, visited, markAll || (isLazyNode && i > 0));
    }
}

void MarkLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop) {
    TNodeSet visited;
    DoMarkLazy(node, lazyNodes, needStop, visited, /*markAll=*/false);
}

void DoMarkNonLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop, TNodeSet& visited) {
    if (!visited.insert(node.Get()).second) {
        return;
    }

    if (needStop(node)) {
        return;
    }

    lazyNodes.erase(node.Get());
    ui32 endIndex = node->IsCallable({"And", "Or", "If", "Coalesce"}) ? 1 : node->ChildrenSize();
    for (ui32 i = 0; i < endIndex; ++i) {
        DoMarkNonLazy(node->ChildPtr(i), lazyNodes, needStop, visited);
    }
}

void MarkNonLazy(const TExprNode::TPtr& node, TNodeSet& lazyNodes, const TExprNodePtrPred& needStop) {
    TNodeSet visited;
    DoMarkNonLazy(node, lazyNodes, needStop, visited);
}

struct TNonStrictCollectResult {
    TNodeSet LazyNonStrictNodes;
    TNodeSet NonStrictNodes;
};

TNonStrictCollectResult CollectLazyNonStrictNodes(const TExprNode::TPtr& lambda) {
    TNodeSet nonStrictNodes;
    VisitExpr(lambda, [&](const TExprNode::TPtr& node) {
        if (node->IsArguments() || node->IsArgument()) {
            return false;
        }

        auto type = node->GetTypeAnn();
        YQL_ENSURE(type);

        // avoid visiting any possible scalar context
        return type->IsComposable();
    }, [&](const TExprNode::TPtr& node) {
        YQL_ENSURE(!nonStrictNodes.contains(node.Get()));
        if (node->IsCallable("AssumeStrict")) {
            return true;
        }
        if (node->IsCallable("AssumeNonStrict")) {
            nonStrictNodes.insert(node.Get());
            return true;
        }
        if (AnyOf(node->ChildrenList(), [&](const auto& child) { return nonStrictNodes.contains(child.Get()); }))
        {
            nonStrictNodes.insert(node.Get());
            return true;
        }

        if (auto maybeStrict = IsStrictNoRecurse(*node); maybeStrict.Defined() && !*maybeStrict) {
            nonStrictNodes.insert(node.Get());
            return true;
        }

        return true;
    });

    auto needStop = MakeBlockRewriteStopPredicate(lambda, /*skipAnyLambdaArguments=*/true, /*skipAllInternalLambdas=*/true);

    TNodeSet lazyNodes;
    MarkLazy(lambda, lazyNodes, needStop);
    MarkNonLazy(lambda, lazyNodes, needStop);

    TNodeSet lazyNonStrict;
    for (auto& node : lazyNodes) {
        if (nonStrictNodes.contains(node)) {
            lazyNonStrict.insert(node);
        }
    }

    return TNonStrictCollectResult{.LazyNonStrictNodes = std::move(lazyNonStrict), .NonStrictNodes = std::move(nonStrictNodes)};
}

class TIfPresentCallableView {
public:
    explicit TIfPresentCallableView(const TExprNode::TPtr node)
        : Node_(node) {
        YQL_ENSURE(Node_->ChildrenSize() >= 3, "Expected at least 3 args for if present.");
        YQL_ENSURE(Lambda()->ChildrenSize() == 2, "Lambda must have exactly one body.");
    }

    TExprNode::TPtr Lambda() const {
        return Node_->ChildPtr(Node_->ChildrenSize() - 2);
    }

    TExprNode::TPtr LambdaBody() const {
        return Lambda()->TailPtr();
    }

    TExprNode::TPtr MissingValue() const {
        return Node_->ChildPtr(Node_->ChildrenSize() - 1);
    }

    TExprNode::TPtr Arg(ui32 index) const {
        Y_ENSURE(index < ArgsSize());
        return Node_->Child(index);
    }

    TExprNode::TListType::size_type ArgsSize() const {
        return Node_->ChildrenSize() - 2;
    }

    TExprNode::TPtr OriginalNode() const {
        return Node_;
    }

private:
    TExprNode::TPtr Node_;
};

class TBlockRewriter {
    using TRewritesMap = TCheckpointHashMap<const TExprNode*, TExprNode::TPtr>;

public:
    explicit TBlockRewriter(TExprContext& ctx, TTypeAnnotationContext& types)
        : Ctx_(ctx)
        , Types_(types)
        , OnUnsupportedTypeCallback_(std::bind(&TBlockRewriter::OnUnsupportedTypeCallback, this, std::placeholders::_1))
    {
    }

    bool CollectBlockRewrites(TTypeAnnotationNode::TConstSpanType inputTypes, bool keepInputColumns, const TExprNode::TPtr& lambda,
                              ui32& newNodes, TNodeMap<size_t>& rewritePositions,
                              TExprNode::TPtr& blockLambda, TExprNode::TPtr& restLambda) {
        newNodes = 0;
        YQL_ENSURE(lambda && lambda->IsLambda());

        if (!Types_.ArrowResolver) {
            return false;
        }

        YQL_ENSURE(inputTypes.size() == lambda->Head().ChildrenSize());

        if (!IsInputTypesAreSupportedByBlocks(inputTypes, lambda->Pos())) {
            return false;
        }

        TExprNode::TListType blockArgs = CreateArgs(lambda->Head().ChildrenSize() + 1, lambda->Pos());

        TRewritesMap rewrites;
        RewriteLambdaArguments(rewrites, blockArgs, lambda);
        const auto [lazyNonStrict, nonStrict] = CollectLazyNonStrictNodes(lambda);

        // clang-format off
        auto stopPredicate = MakeBlockRewriteStopPredicate(lambda, /*skipAnyLambdaArguments=*/true, /*skipAllInternalLambdas=*/true);
        VisitExpr(lambda,
            [&](const TExprNode::TPtr& node) {
                bool shouldContinue = !stopPredicate(node);
                return shouldContinue;
            }, [&rewrites, &lazyNonStrict, &nonStrict, this](const TExprNode::TPtr& node) {
                FigureOutRewriteForEachNode(node, rewrites, lazyNonStrict, nonStrict);
                return true;
            });
        // clang-format on

        // calculate extra columns
        TExprNode::TListType lambdaArgs;
        TExprNode::TListType roots;
        EnsureSameElements(lambda, rewrites, blockArgs);
        if (keepInputColumns) {
            // put original columns first
            lambdaArgs = AddOriginalArgumentsToLambda(lambda, roots, rewritePositions, blockArgs);
        }

        auto subgraphToBlockArgsReplaces = RewriteLambdaBodyWithBlocksAsPossible(lambda, rewrites, roots, lambdaArgs, rewritePositions);

        if (keepInputColumns) {
            AddReplaceForEachLambdaArg(lambda, subgraphToBlockArgsReplaces, lambdaArgs);
        }

        newNodes = CollectRewritedNodesCount(lambda, rewrites);

        YQL_ENSURE(lambdaArgs.size() == roots.size());
        roots.push_back(blockArgs.back());

        blockLambda = Ctx_.NewLambda(lambda->Pos(), Ctx_.NewArguments(lambda->Pos(), std::move(blockArgs)), std::move(roots));

        TExprNode::TListType restRoots;
        for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
            TExprNode::TPtr newRoot;
            auto status = RemapExpr(lambda->ChildPtr(i), newRoot, subgraphToBlockArgsReplaces, Ctx_, TOptimizeExprSettings(&Types_));
            YQL_ENSURE(status != IGraphTransformer::TStatus::Error);
            restRoots.push_back(newRoot);
        }
        restLambda = Ctx_.NewLambda(lambda->Pos(), Ctx_.NewArguments(lambda->Pos(), std::move(lambdaArgs)), std::move(restRoots));

        return true;
    }

private:
    auto Log(const TExprNode::TPtr& node) {
        return TStringBuilder() << "Node: " << node->Content() << ", id: " << node->UniqueId() << ". ";
    }

    ui32 CollectRewritedNodesCount(const TExprNode::TPtr& lambda, TRewritesMap& rewrites) {
        ui64 result = 0;
        auto stopPredicate = MakeBlockRewriteStopPredicate(lambda, /*skipAnyLambdaArguments=*/true, /*skipAllInternalLambdas=*/false);
        VisitExpr(lambda, [&](const TExprNode::TPtr& node) {
            auto rewrite = rewrites.find(node.Get());
            if (rewrite == rewrites.end()) {
                return true;
            }
            auto [fromRewrite, toRewrite] = *rewrite;
            auto shouldStop = stopPredicate(node);
            if (shouldStop) {
                return false;
            }
            if (!fromRewrite->IsArgument() && !fromRewrite->IsArguments()) {
                result++;
                return true;
            }
            return true;
        });
        return result;
    }

    void OnUnsupportedTypeCallback(std::variant<ETypeAnnotationKind, NUdf::EDataSlot> typeKindOrSlot) {
        std::visit([this](const auto& value) { this->Types_.IncNoBlockType(value); }, typeKindOrSlot);
    };

    TMaybe<std::tuple<TExprNode::TListType, TExprNode::TListType, TExprNode::TPtr>> GetBlockArgsForIfPresent(TIfPresentCallableView ifPresentView, const TRewritesMap& rewrites) {
        TExprNode::TListType resultUnwrappedArgs;
        TExprNode::TListType resultExistsArgs;
        for (size_t i = 0; i < ifPresentView.ArgsSize(); ++i) {
            auto rewrited = GetBlockFuncArg(ifPresentView.Arg(i), rewrites);
            if (!rewrited) {
                YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.Arg(i)) << "Lambda arg isn't rewritable. Cannot handle it.";
                return {};
            }

            if (!IsValidValueSupported(ifPresentView.Arg(i)->GetTypeAnn())) {
                YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.Arg(i)) << "Lambda arg is a not supported by default value constructor. Cannot handle it.";
                return {};
            }

            resultExistsArgs.push_back(Ctx_.NewCallable(ifPresentView.Arg(i)->Pos(), "BlockExists", {rewrited}));
            resultUnwrappedArgs.push_back(Ctx_.NewCallable(ifPresentView.Arg(i)->Pos(), "BlockValidUnwrap", {rewrited}));
        }
        auto resultMissingValue = GetBlockFuncArg(ifPresentView.MissingValue(), rewrites);
        if (!resultMissingValue) {
            return {};
        }
        return std::make_tuple(std::move(resultUnwrappedArgs), std::move(resultExistsArgs), std::move(resultMissingValue));
    }

    // Rewrite if present via BlockIf + BlockExists + BlockValidUnwrap.
    TExprNode::TPtr RewriteIfPresent(TIfPresentCallableView ifPresentView, TRewritesMap& rewrites, const TNodeSet& nonStrictNodes) {
        // Check that lambda return type is valid.
        if (!IsSupportedAsBlockType(ifPresentView.Lambda()->Pos(), *ifPresentView.Lambda()->GetTypeAnn(), Ctx_, Types_, /*reportUnspported=*/true)) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.Lambda().Get()) << "Lambda return type is not supported";
            return nullptr;
        }

        TCheckpointGuard guard(rewrites);
        TExprNode::TListType blockArgs = CreateArgs(ifPresentView.ArgsSize(), ifPresentView.Lambda()->Pos());
        RewriteLambdaArguments(rewrites, blockArgs, ifPresentView.Lambda());
        auto wrapResult = GetBlockArgsForIfPresent(ifPresentView, rewrites);
        if (!wrapResult) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.Lambda().Get()) << "Cannot rewrite if present since args are not rewrited";
            return nullptr;
        }

        // clang-format off
        auto stopPredicate = MakeBlockRewriteStopPredicate(ifPresentView.Lambda(), /*skipAnyLambdaArguments=*/true, /*skipAllInternalLambdas=*/true);
        VisitExpr(ifPresentView.Lambda(),
                [&](const TExprNode::TPtr& node) {
                    bool result = !stopPredicate(node);
                    return result;
                },
                [&rewrites, &nonStrictNodes, this](const TExprNode::TPtr& node) {
                    FigureOutRewriteForEachNode(node, rewrites, nonStrictNodes, nonStrictNodes);
                    return true;
                });
        // clang-format on

        if (rewrites.find(ifPresentView.LambdaBody().Get()) == rewrites.end()) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.OriginalNode()) << "Cannot rewrite if present since lambda is not rewrited";
            return nullptr;
        }
        YQL_CLOG(TRACE, CorePeepHole) << Log(ifPresentView.OriginalNode()) << "If present successfully rewrited";

        auto [blockUnwrappedArgs, blockExistsArgs, blockMissingValue] = *wrapResult;

        auto blockLambda = Ctx_.NewLambda(ifPresentView.Lambda()->Pos(), Ctx_.NewArguments(ifPresentView.Lambda()->Pos(), std::move(blockArgs)), {rewrites.Get(ifPresentView.LambdaBody().Get())});

        // clang-format off
        auto calledLambda = Ctx_.Builder(ifPresentView.OriginalNode()->Pos())
                        .Apply(blockLambda)
                            .WithArguments(blockUnwrappedArgs)
                        .Seal()
                        .Build();
        // clang-format on

        // clang-format off
        auto resultLambda = Ctx_.Builder(ifPresentView.OriginalNode()->Pos())
                .Callable("BlockIf")
                    .Add(0, SplitByPairs(ifPresentView.OriginalNode()->Pos(), "BlockAnd", blockExistsArgs, 0, blockExistsArgs.size(), Ctx_))
                    .Add(1, calledLambda)
                    .Add(2, blockMissingValue)
                .Seal()
                .Build();
        // clang-format on

        return resultLambda;
    }

    bool IsInputTypesAreSupportedByBlocks(TTypeAnnotationNode::TConstSpanType inputTypes, TPositionHandle pos) {
        TVector<const TTypeAnnotationNode*> allInputTypes;
        for (const auto& input : inputTypes) {
            if (input->IsBlockOrScalar()) {
                return false;
            }

            allInputTypes.push_back(input);
        }

        auto resolveStatus = Types_.ArrowResolver->AreTypesSupported(Ctx_.GetPosition(pos), allInputTypes, Ctx_, OnUnsupportedTypeCallback_);
        YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
        return resolveStatus == IArrowResolver::OK;
    }

    TExprNode::TListType CreateArgs(size_t size, TPositionHandle pos) {
        TExprNode::TListType blockArgs;
        for (ui32 i = 0; i < size; ++i) { // last argument is used for length of blocks
            blockArgs.push_back(Ctx_.NewArgument(pos, "arg" + ToString(i)));
        }
        return blockArgs;
    }

    void RewriteLambdaArguments(TRewritesMap& rewrites, TExprNode::TListType& blockArgs, TExprNode::TPtr lambda) {
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            rewrites.Set(lambda->Head().Child(i), blockArgs[i]);
        }
    }

    TExprNode::TPtr GetBlockFuncArg(TExprNode::TPtr node, const TRewritesMap& rewrites) {
        if (!node->GetTypeAnn()->IsComputable()) {
            return node;
        } else if (node->IsComplete() && IsSupportedAsBlockType(node->Pos(), *node->GetTypeAnn(), Ctx_, Types_, /*reportUnspported=*/true)) {
            return Ctx_.NewCallable(node->Pos(), "AsScalar", {node});
        } else if (auto rewrited = rewrites.find(node.Get()); rewrited != rewrites.end()) {
            return rewrited->second;
        }
        return nullptr;
    }

    class TIsSuitableScalarRewriteHelper {
        static constexpr ui32 MinVersion = NKikimr::NMiniKQL::TRuntimeVersion::MinSupportedRuntimeVersion;

    public:
        TIsSuitableScalarRewriteHelper()
            : RewriteMap_({
                  {"Guess", 79},
                  {"Way", 80},
                  {"Variant", 81},
                  {"VariantItem", 82},
                  {"DynamicVariant", 83},
                  {"DecimalMul", MinVersion},
                  {"DecimalDiv", MinVersion},
                  {"DecimalMod", MinVersion},
                  {"And", MinVersion},
                  {"Or", MinVersion},
                  {"Xor", MinVersion},
                  {"Not", MinVersion},
                  {"Coalesce", MinVersion},
                  {"Exists", MinVersion},
                  {"If", MinVersion},
                  {"Just", MinVersion},
                  {"AsStruct", MinVersion},
                  {"Member", MinVersion},
                  {"Nth", MinVersion},
                  {"ToPg", MinVersion},
                  {"FromPg", MinVersion},
                  {"PgResolvedCall", MinVersion},
                  {"PgResolvedOp", MinVersion},
                  {"AssumeStrict", MinVersion},
                  {"AssumeNonStrict", MinVersion},
                  {"NoPush", MinVersion},
                  {"Likely", MinVersion},
              })
        {
        }

        bool IsSuitable(const TExprNode::TPtr& node) const {
            if (node->IsList()) {
                return true;
            }

            if (!node->IsCallable()) {
                return false;
            }

            auto it = RewriteMap_.find(node->Content());
            if (it == RewriteMap_.end()) {
                return false;
            }

            return NKikimr::NMiniKQL::RuntimeVersion >= it->second;
        }

    private:
        THashMap<TStringBuf, ui32> RewriteMap_;
    };

    bool IsSuitableForBlockScalarRewrite(const TExprNode::TPtr& node) const {
        return Singleton<TIsSuitableScalarRewriteHelper>()->IsSuitable(node);
    }

    void FigureOutRewriteForEachNode(const TExprNode::TPtr& node, TRewritesMap& rewrites, const TNodeSet& nodesToSkip, const TNodeSet& nonStrictNodes) {
        YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Rewriting node";
        Y_DEFER {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Stop rewriting node";
        };
        if (rewrites.contains(node.Get())) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Node is already rewrited";
            return;
        }

        if (node->IsArguments() || node->IsLambda()) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Skip lambda rewriting";
            return;
        }

        if (node->IsComplete()) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Skip complete node rewriting";
            return;
        }

        if (!node->IsList() && !node->IsCallable()) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Skip non lists and non callables";
            return;
        }

        if (node->IsList() && (!node->GetTypeAnn()->IsComputable() || node->IsLiteralList())) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Skip list that are not computable or literal";
            return;
        }

        if (nodesToSkip.contains(node.Get())) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Skip cause node is explicitly set to skip";
            return;
        }

        if (node->IsCallable("IfPresent")) {
            YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Rewriting if present";
            if (auto rewritedIfPresent = RewriteIfPresent(TIfPresentCallableView(node), rewrites, nonStrictNodes)) {
                rewrites.Set(node.Get(), rewritedIfPresent);
            }
            return;
        }

        TExprNode::TListType funcArgs;
        std::string_view arrowFunctionName;
        if (IsSuitableForBlockScalarRewrite(node)) {
            if (node->IsCallable() && !IsSupportedAsBlockType(node->Pos(), *node->GetTypeAnn(), Ctx_, Types_, /*reportUnspported=*/true)) {
                YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Type are not supported";
                return;
            }

            ui32 startIndex = 0;
            if (node->IsCallable("PgResolvedCall")) {
                if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
                    return;
                }

                startIndex = 3;
            } else if (node->IsCallable("PgResolvedOp")) {
                if (node->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Pg) {
                    return;
                }

                startIndex = 2;
            }

            for (ui32 index = 0; index < startIndex; ++index) {
                auto child = node->ChildPtr(index);
                funcArgs.push_back(child);
            }

            for (ui32 index = startIndex; index < node->ChildrenSize(); ++index) {
                funcArgs.push_back(GetBlockFuncArg(node->Child(index), rewrites));
                if (funcArgs.back() == nullptr) {
                   YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Cannot rewrite arg";
                    return;
                }
            }


            // <AsStruct> arguments (i.e. members of the resulting structure)
            // are literal tuples, that don't propagate their child rewrites.
            // Hence, process these rewrites the following way: wrap the
            // complete expressions, supported by the block engine, with
            // <AsScalar> callable or apply the rewrite of one is found.
            // Otherwise, abort this <AsStruct> rewrite, since one of its
            // arguments is neither block nor scalar.
            if (node->IsCallable("AsStruct")) {
                for (ui32 index = 0; index < node->ChildrenSize(); index++) {
                    auto member = funcArgs[index];
                    auto child = member->TailPtr();
                    TExprNodePtr rewrite;
                    if (child->IsComplete() && IsSupportedAsBlockType(child->Pos(), *child->GetTypeAnn(), Ctx_, Types_, /*reportUnspported=*/true)) {
                        rewrite = Ctx_.NewCallable(child->Pos(), "AsScalar", {child});
                    } else if (auto rit = rewrites.find(child.Get()); rit != rewrites.end()) {
                        rewrite = rit->second;
                    } else {
                        YQL_CLOG(TRACE, CorePeepHole) << Log(node) << "Cannot rewrite AsStruct";
                        return;
                    }
                    funcArgs[index] = Ctx_.NewList(member->Pos(), {member->HeadPtr(), rewrite});
                }
            }

            const bool rewriteAsIs = node->IsCallable({"AssumeStrict", "AssumeNonStrict", "NoPush", "Likely"});
            const TString blockFuncName = rewriteAsIs ? ToString(node->Content()) : (TString("Block") + (node->IsList() ? "AsTuple" : node->Content()));
            if (node->IsCallable({"And", "Or", "Xor"}) && funcArgs.size() > 2) {
                // Split original argument list by pairs (since the order is not important balanced tree is used)
                rewrites.Set(node.Get(), SplitByPairs(node->Pos(), blockFuncName, funcArgs, 0, funcArgs.size(), Ctx_));
            } else {
                rewrites.Set(node.Get(), Ctx_.NewCallable(node->Pos(), blockFuncName, std::move(funcArgs)));
            }
            return;
        }
        const bool isUdf = node->IsCallable("Apply") && node->Head().IsCallable("Udf");
        if (isUdf) {
            if (!GetSetting(*node->Head().Child(7), "blocks")) {
                Types_.IncNoBlockCallable(node->Head().Head().Content());
                return;
            }
        }

        {
            TVector<const TTypeAnnotationNode*> allTypes;
            allTypes.push_back(node->GetTypeAnn());
            for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
                allTypes.push_back(node->Child(i)->GetTypeAnn());
            }

            auto resolveStatus = Types_.ArrowResolver->AreTypesSupported(Ctx_.GetPosition(node->Pos()), allTypes, Ctx_, OnUnsupportedTypeCallback_);
            YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
            if (resolveStatus != IArrowResolver::OK) {
                return;
            }
        }

        TVector<const TTypeAnnotationNode*> argTypes;
        bool hasBlockArg = false;
        for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto child = node->Child(i);
            if (child->IsComplete()) {
                argTypes.push_back(Ctx_.MakeType<TScalarExprType>(child->GetTypeAnn()));
            } else {
                hasBlockArg = true;
                argTypes.push_back(Ctx_.MakeType<TBlockExprType>(child->GetTypeAnn()));
            }
        }

        YQL_ENSURE(!node->IsComplete() && hasBlockArg);
        const TTypeAnnotationNode* outType = Ctx_.MakeType<TBlockExprType>(node->GetTypeAnn());
        if (isUdf) {
            TExprNode::TPtr extraTypes;
            bool renameFunc = false;
            if (node->Head().Child(2)->IsCallable("TupleType")) {
                extraTypes = node->Head().Child(2)->ChildPtr(2);
            } else {
                renameFunc = true;
                extraTypes = Ctx_.NewCallable(node->Head().Pos(), "TupleType", {});
            }

            funcArgs.push_back(Ctx_.Builder(node->Head().Pos())
                .Callable("Udf")
                    .Atom(0, TString(node->Head().Child(0)->Content()) + (renameFunc ? "_BlocksImpl" : ""))
                    .Add(1, node->Head().ChildPtr(1))
                    .Callable(2, "TupleType")
                        .Callable(0, "TupleType")
                                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                    for (ui32 i = 1; i < node->ChildrenSize(); ++i) {
                                        auto type = argTypes[i - 1];
                                        parent.Add(i - 1, ExpandType(node->Head().Pos(), *type, Ctx_));
                                    }

                                    return parent;
                                })
                        .Seal()
                        .Callable(1, "StructType")
                        .Seal()
                        .Add(2, extraTypes)
                    .Seal()
                    .Add(3, node->Head().ChildPtr(3))
                .Seal()
                .Build());

            if (HasSetting(*node->Head().Child(7), "strict")) {
                auto newArg = Ctx_.Builder(node->Head().Pos())
                    .Callable("EnsureStrict")
                        .Add(0, funcArgs.back())
                        .Atom(1, TStringBuilder() << "Block version of " << node->Head().Child(0)->Content() << " is not marked as strict")
                    .Seal()
                    .Build();
                funcArgs.back() = std::move(newArg);
            }
        } else {
            const auto& funcs = TBlockRules::Instance().Funcs;
            auto fit = funcs.find(node->Content());
            if (fit == funcs.end()) {
                Types_.IncNoBlockCallable(node->Content());
                return;
            }

            arrowFunctionName = fit->second.Name;
            funcArgs.push_back(Ctx_.NewAtom(node->Pos(), arrowFunctionName));

            auto resolveStatus = Types_.ArrowResolver->LoadFunctionMetadata(Ctx_.GetPosition(node->Pos()), arrowFunctionName, argTypes, outType, Ctx_);
            YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
            if (resolveStatus != IArrowResolver::OK) {
                Types_.IncNoBlockCallable(node->Content());
                return;
            }
            funcArgs.push_back(ExpandType(node->Pos(), *outType, Ctx_));
        }

        for (ui32 i = isUdf ? 1 : 0; i < node->ChildrenSize(); ++i) {
            auto child = node->Child(i);
            if (child->IsComplete()) {
                funcArgs.push_back(Ctx_.NewCallable(node->Pos(), "AsScalar", {node->ChildPtr(i)}));
            } else {
                auto rit = rewrites.find(child);
                if (rit == rewrites.end()) {
                    return;
                }

                funcArgs.push_back(rit->second);
            }
        }

        rewrites.Set(node.Get(), Ctx_.NewCallable(node->Pos(), isUdf ? "Apply" : "BlockFunc", std::move(funcArgs)));
        return;
    }

    void EnsureSameElements(const TExprNode::TPtr& lambda, const TRewritesMap& rewrites, const TExprNode::TListType& blockArgs) {
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            auto originalArg = lambda->Head().Child(i);
            auto it = rewrites.find(originalArg);
            YQL_ENSURE(it != rewrites.end());
            YQL_ENSURE(it->second == blockArgs[i]);
        }
    }

    TExprNode::TListType AddOriginalArgumentsToLambda(const TExprNode::TPtr& lambda, TExprNode::TListType& roots, TNodeMap<size_t>& rewritePositions, const TExprNode::TListType& blockArgs) {
        TExprNode::TListType originalColumnsArgs;
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            auto arg = Ctx_.NewArgument(lambda->Pos(), "arg" + ToString(originalColumnsArgs.size()));
            auto originalArg = lambda->Head().Child(i);
            originalColumnsArgs.push_back(arg);
            rewritePositions[originalArg] = roots.size();
            roots.push_back(blockArgs[i]);
        }
        return originalColumnsArgs;
    }

    TNodeOnNodeOwnedMap RewriteLambdaBodyWithBlocksAsPossible(const TExprNode::TPtr& lambda, TRewritesMap& rewrites, TExprNode::TListType& roots, TExprNode::TListType& lambdaArgs, TNodeMap<size_t>& rewritePositions) {
        TNodeOnNodeOwnedMap subgraphToBlockArgsReplaces;
        for (ui32 i = 1; i < lambda->ChildrenSize(); ++i) {
            if (lambda->ChildPtr(i)->IsComplete()) {
                auto resolveStatus = Types_.ArrowResolver->AreTypesSupported(Ctx_.GetPosition(lambda->Pos()), {lambda->ChildPtr(i)->GetTypeAnn()}, Ctx_);
                YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
                if (resolveStatus == IArrowResolver::OK) {
                    rewrites.Set(lambda->Child(i), Ctx_.NewCallable(lambda->Pos(), "AsScalar", {lambda->ChildPtr(i)}));
                }
            }

            VisitExpr(lambda->ChildPtr(i), [&](const TExprNode::TPtr& node) {
                auto it = rewrites.find(node.Get());
                if (it != rewrites.end()) {
                    auto& blockedRewrite = it->second;
                    if (!subgraphToBlockArgsReplaces.contains(node.Get())) {
                        auto arg = Ctx_.NewArgument(node->Pos(), "arg" + ToString(lambdaArgs.size()));
                        lambdaArgs.push_back(arg);
                        subgraphToBlockArgsReplaces[node.Get()] = arg;
                        rewritePositions[node.Get()] = roots.size();
                        roots.push_back(blockedRewrite);
                    }

                    return false;
                }

                return true;
            });
        }
        return subgraphToBlockArgsReplaces;
    }

    void AddReplaceForEachLambdaArg(const TExprNode::TPtr& lambda, TNodeOnNodeOwnedMap& subgraphToBlockArgsReplaces, const TExprNode::TListType& lambdaArgs) {
        // add original columns to subgraphToBlockArgsReplaces if not already added
        for (ui32 i = 0; i < lambda->Head().ChildrenSize(); ++i) {
            auto originalArg = lambda->Head().Child(i);
            if (!subgraphToBlockArgsReplaces.contains(originalArg)) {
                subgraphToBlockArgsReplaces[originalArg] = lambdaArgs[i];
            }
        }
    }


    TExprContext& Ctx_;
    TTypeAnnotationContext& Types_;
    IArrowResolver::TUnsupportedTypeCallback OnUnsupportedTypeCallback_;
    TString LogPrefix_;
};

} // namespace

bool CollectBlockRewrites(TExprContext& ctx, TTypeAnnotationContext& types,
    TTypeAnnotationNode::TConstSpanType inputTypes, bool keepInputColumns,
    const TExprNode::TPtr& lambda, ui32& newNodes, TNodeMap<size_t>& rewritePositions,
    TExprNode::TPtr& blockLambda, TExprNode::TPtr& restLambda)
{
    return TBlockRewriter(ctx, types).CollectBlockRewrites(inputTypes, keepInputColumns, lambda, newNodes,
        rewritePositions, blockLambda, restLambda);
}

} // namespace NYql
