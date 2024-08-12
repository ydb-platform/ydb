#include "yql_eval_expr.h"
#include "yql_transform_pipeline.h"
#include "yql_out_transformers.h"

#include <ydb/library/yql/ast/serialize/yql_expr_serialize.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_paths.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/sha.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;

const TString EvaluationComponent = "Evaluation";

static THashSet<TStringBuf> EvaluationFuncs = {
    TStringBuf("EvaluateAtom"),
    TStringBuf("EvaluateExpr"),
    TStringBuf("EvaluateType"),
    TStringBuf("EvaluateCode")
};

static THashSet<TStringBuf> SubqueryExpandFuncs = {
    TStringBuf("SubqueryExtendFor"),
    TStringBuf("SubqueryUnionAllFor"),
    TStringBuf("SubqueryMergeFor"),
    TStringBuf("SubqueryUnionMergeFor"),
    TStringBuf("SubqueryOrderBy"),
    TStringBuf("SubqueryAssumeOrderBy")
};

TString MakeCacheKey(const TExprNode& root, TExprContext& ctx) {
    TConvertToAstSettings settings;
    settings.NormalizeAtomFlags = true;
    settings.AllowFreeArgs = false;
    settings.RefAtoms = true;
    settings.NoInlineFunc = [](const TExprNode&) { return true; };
    auto ast = ConvertToAst(root, ctx, settings);
    YQL_ENSURE(ast.Root);
    auto str = ast.Root->ToString();
    SHA256_CTX sha;
    SHA256_Init(&sha);
    SHA256_Update(&sha, str.Data(), str.Size());
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &sha);
    return TString((const char*)hash, sizeof(hash));
}

bool CheckPendingArgs(const TExprNode& root, TNodeSet& visited, TNodeMap<const TExprNode*>& activeArgs, const TNodeMap<ui32>& externalWorlds, TExprContext& ctx,
    bool underTypeOf, bool& hasUnresolvedTypes) {
    if (!visited.emplace(&root).second) {
        return true;
    }

    if (root.IsCallable({"TypeOf", "SqlColumnOrType", "SqlPlainColumnOrType"})) {
        underTypeOf = true;
    }

    if (root.Type() == TExprNode::Argument) {
        if (activeArgs.find(&root) == activeArgs.cend()) {
            if (underTypeOf) {
                if (!root.GetTypeAnn() && !externalWorlds.count(&root)) {
                    hasUnresolvedTypes = true;
                }
            } else if (!externalWorlds.count(&root)) {
                ctx.AddError(TIssue(ctx.GetPosition(root.Pos()), TStringBuilder() << "Failed to evaluate unresolved argument: " << root.Content() << ". Did you use a column?"));
                return false;
            }
        }
    }

    if (root.Type() == TExprNode::Lambda) {
        root.Child(0)->ForEachChild([&](const TExprNode& arg) {
            if (!activeArgs.emplace(&arg, &root).second) {
                ythrow yexception() << "argument is duplicated, #" << arg.UniqueId();
            }
        });
    }

    for (ui32 index = 0; index < root.ChildrenSize(); ++index) {
        const auto& child = *root.Child(index);
        auto onlyType = underTypeOf || (root.IsCallable("MatchType") || root.IsCallable("IfType")) && (index == 0);
        if (!CheckPendingArgs(child, visited, activeArgs, externalWorlds, ctx, onlyType,
            hasUnresolvedTypes)) {
            return false;
        }
    };

    return true;
}

class TMarkReachable {
public:
    TNodeSet Reachable;
    TNodeMap<ui32> ExternalWorlds;
    TDeque<TExprNode::TPtr> ExternalWorldsList;
    bool HasConfigPending = false;

public:
    void Scan(const TExprNode& node) {
        VisitExprByFirst(node, [this](const TExprNode& n) {
            if (n.IsCallable(ConfigureName)) {
                if (n.ChildrenSize() > 3 && n.Child(1)->Child(0)->Content() == ConfigProviderName) {
                    bool pending = false;
                    for (size_t i = 3; i < n.ChildrenSize(); ++i) {
                        if (n.Child(i)->IsCallable("EvaluateAtom")) {
                            pending = true;
                            break;
                        }
                    }
                    if (pending) {
                        const TStringBuf command = n.Child(2)->Content();
                        if (command == "AddFileByUrl") {
                            PendingFileAliases.insert(n.Child(3)->Content());
                        } else if (command == "AddFolderByUrl") {
                            PendingFolderPrefixes.insert(n.Child(3)->Content());
                        }
                    }
                }
            }
            return true;
        });
        ScanImpl(node);
    }

private:
    void ScanImpl(const TExprNode& node) {
        if (!Visited.emplace(&node).second) {
            return;
        }

        if (node.IsCallable("Seq!")) {
            for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
                auto lambda = node.Child(i);
                auto arg = lambda->Child(0)->ChildPtr(0);
                ui32 id = ExternalWorlds.size();
                YQL_ENSURE(ExternalWorlds.emplace(arg.Get(), id).second);
                ExternalWorldsList.push_back(arg);
            }
        }

        static THashSet<TStringBuf> FILE_CALLABLES = {"FilePath", "FileContent", "FolderPath"};
        if (node.IsCallable(FILE_CALLABLES)) {
            const auto alias = node.Head().Content();
            if (PendingFileAliases.contains(alias) || AnyOf(PendingFolderPrefixes, [alias](const TStringBuf prefix) {
                auto withSlash = TString(prefix) + "/";
                return alias.StartsWith(withSlash);
                })) {
                for (auto& curr: CurrentEvalNodes) {
                    Reachable.erase(curr);
                }
                HasConfigPending = true;
            }
        }

        if (node.IsCallable("QuoteCode")) {
            Reachable.insert(&node);
            return;
        }

        bool pop = false;
        if (node.IsCallable(EvaluationFuncs) || node.IsCallable(SubqueryExpandFuncs)) {
            Reachable.insert(&node);
            CurrentEvalNodes.insert(&node);
            pop = true;
        }

        if (node.IsCallable({ "EvaluateIf!", "EvaluateFor!", "EvaluateParallelFor!" })) {
            // scan predicate/list only
            if (node.ChildrenSize() > 1) {
                CurrentEvalNodes.insert(&node);
                pop = true;
                ScanImpl(*node.Child(1));
            }
        } else if (node.IsCallable(SubqueryExpandFuncs)) {
            // scan list only if it's wrapped by evaluation func
            ui32 index = 0;
            if (node.IsCallable("SubqueryOrderBy") || node.IsCallable("SubqueryAssumeOrderBy")) {
                index = 1;
            }
            if (node.ChildrenSize() > index) {
                if (node.Child(index)->IsCallable(EvaluationFuncs)) {
                    CurrentEvalNodes.insert(&node);
                    pop = true;
                    ScanImpl(*node.Child(index));
                } else {
                    for (const auto& child : node.Children()) {
                        ScanImpl(*child);
                    }
                }
            }
        } else {
            for (const auto& child : node.Children()) {
                ScanImpl(*child);
            }
        }
        if (pop) {
            CurrentEvalNodes.erase(&node);
        }
    }

private:
    TNodeSet Visited;
    THashSet<TStringBuf> PendingFileAliases;
    THashSet<TStringBuf> PendingFolderPrefixes;
    TNodeSet CurrentEvalNodes;
};

struct TEvalScope {
    TEvalScope(TTypeAnnotationContext& types)
        : Types(types)
    {
        ++Types.EvaluationInProgress;
        for (auto& dataProvider : Types.DataSources) {
            dataProvider->EnterEvaluation(Types.EvaluationInProgress);
        }
    }

    ~TEvalScope() {
        for (auto& dataProvider : Types.DataSources) {
            dataProvider->LeaveEvaluation(Types.EvaluationInProgress);
        }
        --Types.EvaluationInProgress;
    }
    TTypeAnnotationContext& Types;
};

bool ValidateCalcWorlds(const TExprNode& node, const TTypeAnnotationContext& types, TNodeSet& visited) {
    if (!visited.emplace(&node).second) {
        return true;
    }

    if (node.Type() == TExprNode::World) {
        return true;
    }

    if (node.IsCallable("Commit!") || node.IsCallable("CommitAll!") || node.IsCallable("Configure!")) {
        return ValidateCalcWorlds(*node.Child(0), types, visited);
    }

    if (node.IsCallable("Sync!")) {
        for (const auto& child : node.Children()) {
            if (!ValidateCalcWorlds(*child, types, visited)) {
                return false;
            }
        }

        return true;
    }

    for (auto& dataProvider : types.DataSources) {
        if (dataProvider->CanEvaluate(node)) {
            return true;
        }
    }

    return false;
}

TExprNode::TPtr QuoteCode(const TExprNode::TPtr& node, TExprContext& ctx, TNodeOnNodeOwnedMap& knownArgs, TNodeOnNodeOwnedMap& visited,
    const TNodeMap<ui32>& externalWorlds) {
    auto& res = visited[node.Get()];
    if (res) {
        return res;
    }

    switch (node->Type()) {
    case TExprNode::Atom: {
        return res = ctx.Builder(node->Pos())
            .Callable("AtomCode")
                .Callable(0, "String")
                    .Atom(0, node->Content())
                .Seal()
            .Seal()
            .Build();
    }

    case TExprNode::Argument: {
        auto it = knownArgs.find(node.Get());
        if (it != knownArgs.end()) {
            return res = it->second;
        }

        auto externalWorldIt = externalWorlds.find(node.Get());
        if (externalWorldIt != externalWorlds.end()) {
            return ctx.Builder(node->Pos())
                .Callable("FuncCode")
                    .Callable(0, "String")
                        .Atom(0, "WorldArg")
                    .Seal()
                    .Callable(1, "AtomCode")
                        .Callable(0, "String")
                            .Atom(0, ToString(externalWorldIt->second))
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        return res = ctx.Builder(node->Pos())
            .Callable("ReprCode")
                .Add(0, node)
            .Seal()
            .Build();
    }

    case TExprNode::List: {
        TExprNode::TListType children;
        children.reserve(node->ChildrenSize());
        for (auto& child : node->Children()) {
            auto childCode = QuoteCode(child, ctx, knownArgs, visited, externalWorlds);
            if (!childCode) {
                return nullptr;
            }

            children.push_back(childCode);
        }

        return res = ctx.NewCallable(node->Pos(), "ListCode", std::move(children));
    }

    case TExprNode::Callable: {
        TExprNode::TListType children;
        children.reserve(node->ChildrenSize() + 1);
        children.push_back(ctx.Builder(node->Pos())
            .Callable("String")
                .Atom(0, node->Content())
            .Seal()
            .Build());

        for (auto& child : node->Children()) {
            auto childCode = QuoteCode(child, ctx, knownArgs, visited, externalWorlds);
            if (!childCode) {
                return nullptr;
            }

            children.push_back(childCode);
        }

        return res = ctx.NewCallable(node->Pos(), "FuncCode", std::move(children));
    }

    case TExprNode::Lambda: {
        TExprNode::TListType lambdaArgsItems;
        for (auto arg : node->Child(0)->Children()) {
            auto lambdaArg = ctx.NewArgument(arg->Pos(), arg->Content());
            lambdaArgsItems.push_back(lambdaArg);
            knownArgs.emplace(arg.Get(), lambdaArg);
        }

        auto lambdaArgs = ctx.NewArguments(node->Pos(), std::move(lambdaArgsItems));
        auto body = QuoteCode(node->ChildPtr(1), ctx, knownArgs, visited, externalWorlds);
        if (!body) {
            return nullptr;
        }

        for (auto arg : node->Child(0)->Children()) {
            knownArgs.erase(arg.Get());
        }

        auto lambda = ctx.NewLambda(node->Pos(), std::move(lambdaArgs), std::move(body));
        return res = ctx.Builder(node->Pos())
            .Callable("LambdaCode")
                .Add(0, lambda)
            .Seal()
            .Build();
    }

    case TExprNode::World: {
        return res = ctx.Builder(node->Pos())
            .Callable("WorldCode")
            .Seal()
            .Build();
    }

    default:
        YQL_ENSURE(false, "Unknown type: " << node->Type());
    }
}

IGraphTransformer::TStatus EvaluateExpression(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TTypeAnnotationContext& types, TExprContext& ctx, const IFunctionRegistry& functionRegistry, IGraphTransformer* calcTransfomer) {
    output = input;
    if (ctx.Step.IsDone(TExprStep::ExprEval))
        return IGraphTransformer::TStatus::Ok;

    YQL_CLOG(DEBUG, CoreEval) << "EvaluateExpression - start";
    bool pure = false;
    TString nextProvider;
    TMaybe<IDataProvider*> calcProvider;
    TExprNode::TPtr calcWorldRoot;
    TPositionHandle pipelinePos;
    bool isAtomPipeline = false;
    bool isOptionalAtom = false;
    bool isTypePipeline = false;
    bool isCodePipeline = false;
    TTransformationPipeline pipeline(&types);
    pipeline.AddServiceTransformers();
    pipeline.AddPreTypeAnnotation();
    pipeline.AddExpressionEvaluation(functionRegistry);
    pipeline.AddIOAnnotation();
    pipeline.AddTypeAnnotationTransformer();
    pipeline.Add(CreateFunctorTransformer(
        [&](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
        output = input;
        if (!input->GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Lambda is not allowed as argument for function: " << input->Content()));
            return IGraphTransformer::TStatus::Error;
        }

        if (isAtomPipeline) {
            const TDataExprType* dataType;
            if (!EnsureDataOrOptionalOfData(*input, isOptionalAtom, dataType, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureSpecificDataType(input->Pos(), *dataType, EDataSlot::String, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else if (isTypePipeline) {
            if (!EnsureSpecificDataType(*input, EDataSlot::Yson, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else if (isCodePipeline) {
            if (!EnsureSpecificDataType(*input, EDataSlot::String, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsurePersistable(*input, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        return IGraphTransformer::TStatus::Ok;
    }), "TopLevelType", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR, "Ensure type of expression is correct");
    const bool forSubGraph = true;
    pipeline.AddPostTypeAnnotation(forSubGraph);
    pipeline.Add(TExprLogTransformer::Sync("EvalExpressionOpt", NLog::EComponent::CoreEval, NLog::ELevel::TRACE),
        "EvalOptTrace", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR, "EvalOptTrace");
    pipeline.AddOptimization(false);
    pipeline.Add(CreateFunctorTransformer(
        [&](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) -> IGraphTransformer::TStatus {
        output = input;
        if (!calcProvider) {
            pure = false;
            if (IsPureIsolatedLambda(*input)) {
                pure = true;
                if (calcTransfomer) {
                    calcProvider.ConstructInPlace();
                } else {
                    if (nextProvider.empty()) {
                        nextProvider = types.GetDefaultDataSource();
                    }
                    if (!nextProvider.empty() &&
                        types.DataSourceMap.contains(nextProvider)) {
                        calcProvider = types.DataSourceMap[nextProvider].Get();
                    }
                }
            } else if (!calcTransfomer) {
                for (auto& p : types.DataSources) {
                    TSyncMap syncList;
                    if (p->CanBuildResult(*input, syncList)) {
                        bool canExec = true;
                        for (auto& x : syncList) {
                            if (x.first->Type() == TExprNode::World) {
                                continue;
                            }

                            if (!p->GetExecWorld(x.first, calcWorldRoot)) {
                                canExec = false;
                                break;
                            }

                            if (!calcWorldRoot) {
                                continue;
                            }

                            TNodeSet visited;
                            if (!ValidateCalcWorlds(*calcWorldRoot, types, visited)) {
                                canExec = false;
                                break;
                            }
                        }

                        if (canExec) {
                            calcProvider = p.Get();
                            output = (*calcProvider.Get())->CleanupWorld(input, ctx);
                            if (!output) {
                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Error);
                            }

                            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                        }
                    }
                }
            }

            if (!calcProvider) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Only pure expressions are supported"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!calcWorldRoot) {
            calcWorldRoot = ctx.NewWorld(input->Pos());
            calcWorldRoot->SetTypeAnn(ctx.MakeType<TUnitExprType>());
            calcWorldRoot->SetState(TExprNode::EState::ConstrComplete);
        }

        return IGraphTransformer::TStatus::Ok;
    }), "CheckPure", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR, "Ensure expression is computable");

    pipeline.Add(MakePeepholeOptimization(&types), "PeepHole", EYqlIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR, "Peephole optimizations");

    auto fullTransformer = pipeline.Build();

    TMarkReachable marked;
    marked.Scan(*output);

    THashSet<TStringBuf> modifyCallables;
    modifyCallables.insert(WriteName);
    modifyCallables.insert(ConfigureName);
    modifyCallables.insert(CommitName);
    modifyCallables.insert("CommitAll!");
    for (auto& dataSink: types.DataSinks) {
        dataSink->FillModifyCallables(modifyCallables);
    }

    IGraphTransformer::TStatus hasPendingEvaluations = IGraphTransformer::TStatus::Ok;
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    auto status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx)->TExprNode::TPtr {
        TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
            return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()), TStringBuilder() << "At function: " << node->Content());
        });

        if (node->IsCallable("EvaluateIf!")) {
            if (!EnsureMinArgsCount(*node, 3, ctx)) {
                return nullptr;
            }

            if (!EnsureMaxArgsCount(*node, 4, ctx)) {
                return nullptr;
            }

            if (!EnsureLambda(*node->Child(2), ctx) || !EnsureArgsCount(*node->Child(2)->Child(0), 1, ctx)) {
                return nullptr;
            }

            if (node->ChildrenSize() == 4) {
                if (!EnsureLambda(*node->Child(3), ctx) || !EnsureArgsCount(*node->Child(3)->Child(0), 1, ctx)) {
                    return nullptr;
                }
            }

            if (node->Child(1)->IsCallable(EvaluationFuncs)) {
                return node;
            }

            if (!node->Child(1)->IsCallable("Bool") || node->Child(1)->ChildrenSize() != 1) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(1)->Pos()), TStringBuilder() << "Expected literal bool"));
                return nullptr;
            }

            auto predAtom = node->Child(1)->Child(0);
            ui8 predValue;
            if (predAtom->Flags() & TNodeFlags::BinaryContent) {
                if (predAtom->Content().size() != 1) {
                    ctx.AddError(TIssue(ctx.GetPosition(predAtom->Pos()), TStringBuilder() << "Incorrect literal bool value"));
                    return nullptr;
                }

                predValue = *(const ui8*)predAtom->Content().data();
            } else {
                predValue = (predAtom->Content() == "true" || predAtom->Content() == "1");
            }

            if (predValue) {
                return ctx.Builder(node->Pos())
                    .Apply(node->ChildPtr(2))
                        .With(0, node->ChildPtr(0))
                    .Seal()
                    .Build();
            } else if (node->ChildrenSize() == 4) {
                return ctx.Builder(node->Pos())
                    .Apply(node->ChildPtr(3))
                        .With(0, node->ChildPtr(0))
                    .Seal()
                    .Build();
            } else {
                return node->ChildPtr(0);
            }
        }

        if (node->IsCallable({"EvaluateFor!", "EvaluateParallelFor!"})) {
            const bool seq = node->IsCallable("EvaluateFor!");
            if (!EnsureMinArgsCount(*node, 3, ctx)) {
                return nullptr;
            }

            if (!EnsureMaxArgsCount(*node, 4, ctx)) {
                return nullptr;
            }

            if (!EnsureLambda(*node->Child(2), ctx) || !EnsureArgsCount(*node->Child(2)->Child(0), 2, ctx)) {
                return nullptr;
            }

            if (node->ChildrenSize() == 4) {
                if (!EnsureLambda(*node->Child(3), ctx) || !EnsureArgsCount(*node->Child(3)->Child(0), 1, ctx)) {
                    return nullptr;
                }
            }

            auto list = node->Child(1);
            if (list->IsCallable(EvaluationFuncs)) {
                return node;
            }

            bool noData = false;
            if (list->IsCallable("Just")) {
                if (!EnsureArgsCount(*list, 1, ctx)) {
                    return nullptr;
                }

                list = list->Child(0);
            } else if (list->IsCallable("Nothing") || list->IsCallable("Null")) {
                noData = true;
            }

            if (!noData && list->IsCallable("List") && list->ChildrenSize() == 1) {
                noData = true;
            }

            if (noData) {
                if (node->ChildrenSize() == 4) {
                    return ctx.Builder(node->Pos())
                        .Apply(node->ChildPtr(3))
                            .With(0, node->ChildPtr(0))
                        .Seal()
                        .Build();
                } else {
                    return node->ChildPtr(0);
                }
            }

            if (!list->IsCallable("List") && !list->IsCallable("AsList")) {
                ctx.AddError(TIssue(ctx.GetPosition(list->Pos()), TStringBuilder() << "Expected (optional) literal list"));
                return nullptr;
            }

            auto itemsCount = list->ChildrenSize() - (list->IsCallable("List") ? 1 : 0);
            const auto limit = seq ? types.EvaluateForLimit : types.EvaluateParallelForLimit;
            if (itemsCount > limit) {
                ctx.AddError(TIssue(ctx.GetPosition(list->Pos()), TStringBuilder() << "Too large list for EVALUATE " << (seq ? "" : "PARALLEL ") << "FOR, allowed: " <<
                    limit << ", got: " << itemsCount));
                return nullptr;
            }

            auto world = node->ChildPtr(0);
            auto ret = ctx.Builder(node->Pos())
                .Callable(seq ? "Seq!" : "Sync!")
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        ui32 pos = 0;
                        if (seq) {
                            parent.Add(pos++, world);
                        }

                        for (ui32 i = list->IsCallable("List") ? 1 : 0; i < list->ChildrenSize(); ++i) {
                            auto arg = seq ? ctx.NewArgument(node->Pos(), "world") : world;
                            auto body = ctx.Builder(node->Pos())
                                .Apply(node->ChildPtr(2))
                                    .With(0, arg)
                                    .With(1, list->ChildPtr(i))
                                .Seal()
                                .Build();

                            if (seq) {
                                auto lambda = ctx.NewLambda(node->Pos(), ctx.NewArguments(node->Pos(), { arg }), std::move(body));
                                parent.Add(pos++, lambda);
                            } else {
                                parent.Add(pos++, body);
                            }
                        }

                        return parent;
                    })
                .Seal()
                .Build();

            ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
            hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
            return ret;
        }

        if (node->IsCallable(SubqueryExpandFuncs)) {
            if (!EnsureArgsCount(*node, 2, ctx)) {
                return nullptr;
            }

            if (node->IsCallable("SubqueryOrderBy") || node->IsCallable("SubqueryAssumeOrderBy")) {
                auto inputSub = node->Child(0);
                if (inputSub->IsArgument()) {
                    return node;
                }

                if (!EnsureLambda(*inputSub, ctx) || !EnsureArgsCount(*inputSub->Child(0), 1, ctx)) {
                    return nullptr;
                }

                auto keys = node->Child(1);
                if (keys->IsCallable(EvaluationFuncs)) {
                    return node;
                }

                if (!keys->IsCallable("AsList") && !keys->IsCallable("List") && !keys->IsCallable("EmptyList")) {
                    ctx.AddError(TIssue(ctx.GetPosition(keys->Pos()), TStringBuilder() << "Expected literal list"));
                    return nullptr;
                }

                auto itemsCount = keys->ChildrenSize() - (keys->IsCallable("List") ? 1 : 0);
                if (itemsCount > types.EvaluateOrderByColumnLimit) {
                    ctx.AddError(TIssue(ctx.GetPosition(keys->Pos()), TStringBuilder() << "Too many columns for subquery order by, allowed: " <<
                        types.EvaluateOrderByColumnLimit << ", got: " << itemsCount));
                    return nullptr;
                }

                auto arg = ctx.NewArgument(node->Pos(), "row");

                TExprNode::TListType dirItems;
                TExprNode::TListType extractorItems;
                for (ui32 i = keys->IsCallable("List") ? 1 : 0; i < keys->ChildrenSize(); ++i) {
                    auto k = keys->Child(i);
                    if (!k->IsList() || k->ChildrenSize() != 2) {
                        ctx.AddError(TIssue(ctx.GetPosition(k->Pos()), TStringBuilder() << "Expected tuple of 2 items"));
                        return nullptr;
                    }

                    auto columnName = k->Child(0);
                    auto direction = k->Child(1);
                    if (!columnName->IsCallable("String")) {
                        ctx.AddError(TIssue(ctx.GetPosition(columnName->Pos()), TStringBuilder() << "Expected String as column name"));
                        return nullptr;
                    }

                    if (!direction->IsCallable("Bool")) {
                        ctx.AddError(TIssue(ctx.GetPosition(columnName->Pos()), TStringBuilder() << "Expected Bool as direction"));
                        return nullptr;
                    }

                    dirItems.push_back(direction);
                    extractorItems.push_back(ctx.Builder(k->Pos())
                        .Callable("Member")
                            .Add(0, arg)
                            .Add(1, columnName->ChildPtr(0))
                        .Seal()
                        .Build());
                }

                auto args = ctx.NewArguments(node->Pos(), { arg });
                auto body = ctx.NewList(node->Pos(), std::move(extractorItems));
                auto extractorLambda = ctx.NewLambda(node->Pos(), std::move(args), std::move(body));

                auto dirs = ctx.NewList(node->Pos(), std::move(dirItems));
                auto sorted = ctx.Builder(node->Pos())
                    .Lambda()
                        .Param("world")
                        .Callable(node->IsCallable("SubqueryOrderBy") ? "Sort" : "AssumeSorted")
                            .Apply(0, inputSub)
                                .With(0, "world")
                            .Seal()
                            .Add(1, dirs)
                            .Add(2, extractorLambda)
                        .Seal()
                    .Seal()
                    .Build();

                ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
                hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
                return sorted;
            } else {
                auto list = node->Child(0);
                if (list->IsCallable(EvaluationFuncs)) {
                    return node;
                }

                if (list->IsCallable("Just")) {
                    list = list->Child(0);
                }

                if (!list->IsCallable("AsList") || list->ChildrenSize() == 0) {
                    ctx.AddError(TIssue(ctx.GetPosition(list->Pos()), TStringBuilder() << "Expected non-empty literal list"));
                    return nullptr;
                }

                auto itemsCount = list->ChildrenSize();
                if (itemsCount > types.EvaluateParallelForLimit) {
                    ctx.AddError(TIssue(ctx.GetPosition(list->Pos()), TStringBuilder() << "Too large list for subquery loop, allowed: " <<
                        types.EvaluateParallelForLimit << ", got: " << itemsCount));
                    return nullptr;
                }

                if (node->Child(1)->IsCallable(EvaluationFuncs)) {
                    return node;
                }

                const auto status = ConvertToLambda(node->ChildRef(1), ctx, 2, 2, false);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return nullptr;
                }

                const auto& lambda = node->Child(1);
                TExprNodeList argItems;
                argItems.push_back(ctx.NewArgument(node->Pos(), "world"));

                TExprNodeList inputs;
                for (ui32 i = 0; i < list->ChildrenSize(); ++i) {
                    TNodeOnNodeOwnedMap replaces;
                    replaces[lambda->Child(0)->Child(0)] = argItems[0];
                    replaces[lambda->Child(0)->Child(1)] = list->ChildPtr(i);

                    inputs.push_back(ctx.ReplaceNodes(lambda->TailPtr(), replaces));
                }

                auto body = ctx.NewCallable(node->Pos(), node->Content().substr(8, node->Content().size() - 8 - 3), std::move(inputs));
                auto args = ctx.NewArguments(node->Pos(), std::move(argItems));
                auto merged = ctx.NewLambda(node->Pos(), std::move(args), std::move(body));

                ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
                hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
                return merged;
            }
        }

        if (node->IsCallable("MrTableEach") || node->IsCallable("MrTableEachStrict")) {
            TExprNode::TListType keys;
            TStringBuf prefix;
            if (node->ChildrenSize() != 0 && node->TailPtr()->IsAtom()) {
                prefix = node->TailPtr()->Content();
            }
            for (const auto& eachKey : node->Children()) {
                if (eachKey->IsAtom()) {
                    continue;
                }

                if (!eachKey->IsCallable("Key")) {
                    ctx.AddError(TIssue(ctx.GetPosition(eachKey->Pos()), TStringBuilder() << "Expected Key"));
                    return nullptr;
                }

                if (!EnsureMinArgsCount(*eachKey, 1, ctx)) {
                    return nullptr;
                }

                if (!eachKey->Child(0)->IsList() || eachKey->Child(0)->ChildrenSize() != 2 ||
                    !eachKey->Child(0)->Child(0)->IsAtom() || eachKey->Child(0)->Child(0)->Content() != "table") {
                    ctx.AddError(TIssue(ctx.GetPosition(eachKey->Pos()), TStringBuilder() << "Invalid Key"));
                    return nullptr;
                }

                auto list = eachKey->Child(0)->Child(1);
                if (list->IsCallable(EvaluationFuncs)) {
                    return node;
                }

                if (list->IsCallable("List") && list->ChildrenSize() == 0) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Invalid literal list value"));
                    return nullptr;
                }

                if (!list->IsCallable("List") && !list->IsCallable("AsList")) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected literal list"));
                    return nullptr;
                }

                for (ui32 i = list->IsCallable("List") ? 1 : 0; i < list->ChildrenSize(); ++i) {
                    auto name = list->ChildPtr(i);
                    if (!name->IsCallable("String")) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Expected literal string as table name"));
                        return nullptr;
                    }
                    if (prefix) {
                        name = ctx.ChangeChild(*name, 0, ctx.NewAtom(node->Pos(), BuildTablePath(prefix, name->Child(0)->Content())));
                    }
                    keys.push_back(ctx.ReplaceNode(TExprNode::TPtr(eachKey), *list, std::move(name)));
                }
            }

            return node->IsCallable("MrTableEach") ?
                ctx.NewCallable(node->Pos(), "MrTableConcat", std::move(keys)) :
                ctx.NewList(node->Pos(), std::move(keys));
        }

        if (node->IsCallable("QuoteCode")) {
            if (marked.Reachable.find(node.Get()) == marked.Reachable.cend()) {
                hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus::Repeat);
                return node;
            }

            if (!EnsureArgsCount(*node, 1, ctx)) {
                return nullptr;
            }

            TNodeOnNodeOwnedMap knownArgs;
            TNodeOnNodeOwnedMap visited;
            return QuoteCode(node->ChildPtr(0), ctx, knownArgs, visited, marked.ExternalWorlds);
        }

        if (!node->IsCallable(EvaluationFuncs)) {
            return node;
        }

        if (!EnsureArgsCount(*node, 1, ctx)) {
            return nullptr;
        }

        if (marked.Reachable.find(node.Get()) == marked.Reachable.cend()) {
            if (marked.HasConfigPending) {
                ctx.Step.Repeat(TExprStep::Configure);
            }
            hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, marked.HasConfigPending));
            return node;
        }

        auto newArg = node->ChildPtr(0);
        {
            TNodeSet visited;
            TNodeMap<const TExprNode*> activeArgs;
            bool hasUnresolvedTypes = false;
            if (!CheckPendingArgs(*newArg, visited, activeArgs, marked.ExternalWorlds, ctx, false, hasUnresolvedTypes)) {
                return nullptr;
            }

            if (hasUnresolvedTypes) {
                YQL_CLOG(DEBUG, CoreEval) << "EvaluateExpression - has unresolved types";
                return node;
            }
        }

        TNodeOnNodeOwnedMap externalWorldReplaces;
        for (auto& x : marked.ExternalWorlds) {
            externalWorldReplaces.emplace(x.first, ctx.NewWorld(x.first->Pos()));
        }

        newArg = ctx.ReplaceNodes(std::move(newArg), externalWorldReplaces);
        TExprNode::TPtr clonedArg;
        {
            TNodeOnNodeOwnedMap deepClones;
            clonedArg = ctx.DeepCopy(*newArg, ctx, deepClones, false, true, true);
        }

        // trim modifications
        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(clonedArg, clonedArg, [&](const TExprNode::TPtr& node, TExprContext& ctx) {
            Y_UNUSED(ctx);
            if (node->IsCallable(modifyCallables) && node->ChildrenSize() > 0) {
                return node->ChildPtr(0);
            }

            return node;
        }, ctx, settings);

        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return nullptr;
        }

        pipelinePos = node->Pos();
        isAtomPipeline = node->IsCallable("EvaluateAtom");
        isTypePipeline = node->IsCallable("EvaluateType");
        isCodePipeline = node->IsCallable("EvaluateCode");
        isOptionalAtom = false;
        if (isTypePipeline) {
            clonedArg = ctx.NewCallable(clonedArg->Pos(), "SerializeTypeHandle", { clonedArg });
        } else if (isCodePipeline) {
            clonedArg = ctx.NewCallable(clonedArg->Pos(), "SerializeCode", { clonedArg });
        }

        NYT::TNode ysonNode;
        do {
            calcProvider.Clear();
            calcWorldRoot.Drop();
            fullTransformer->Rewind();
            auto prevSteps = ctx.Step;
            TEvalScope scope(types);
            ctx.Step.Reset();
            if (prevSteps.IsDone(TExprStep::Recapture)) {
                ctx.Step.Done(TExprStep::Recapture);
            }
            status = SyncTransform(*fullTransformer, clonedArg, ctx);
            ctx.Step = prevSteps;
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return nullptr;
            }

            // execute calcWorldRoot
            auto execTransformer = CreateExecutionTransformer(types, [](const TOperationProgress&){}, false);
            status = SyncTransform(*execTransformer, calcWorldRoot, ctx);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return nullptr;
            }

            IDataProvider::TFillSettings fillSettings;
            auto delegatedNode = Build<TResult>(ctx, node->Pos())
                .Input(clonedArg)
                .BytesLimit()
                    .Value(TString())
                .Build()
                .RowsLimit()
                    .Value(TString())
                .Build()
                .FormatDetails()
                    .Value(ToString((ui32)NYson::EYsonFormat::Binary))
                .Build()
                .Settings().Build()
                .Format()
                    .Value(ToString((ui32)IDataProvider::EResultFormat::Yson))
                .Build()
                .PublicId()
                    .Value(TString())
                .Build()
                .Discard()
                    .Value("false")
                .Build()
                .Origin(calcWorldRoot)
                .Done().Ptr();

            auto atomType = ctx.MakeType<TUnitExprType>();
            for (auto idx: {TResOrPullBase::idx_BytesLimit, TResOrPullBase::idx_RowsLimit, TResOrPullBase::idx_FormatDetails,
                TResOrPullBase::idx_Format, TResOrPullBase::idx_PublicId, TResOrPullBase::idx_Discard, TResOrPullBase::idx_Settings }) {
                delegatedNode->Child(idx)->SetTypeAnn(atomType);
                delegatedNode->Child(idx)->SetState(TExprNode::EState::ConstrComplete);
            }

            delegatedNode->SetTypeAnn(atomType);
            delegatedNode->SetState(TExprNode::EState::ConstrComplete);
            TString yson;
            TString key;
            if (types.QContext) {
                key = MakeCacheKey(*clonedArg, ctx);
            }

            if (types.QContext.CanRead()) {
                auto item = types.QContext.GetReader()->Get({EvaluationComponent, key}).GetValueSync();
                if (!item) {
                    throw yexception() << "Missing replay data";
                }

                yson = item->Value;
            } else {
                auto& transformer = calcTransfomer ? *calcTransfomer : (*calcProvider.Get())->GetCallableExecutionTransformer();
                status = SyncTransform(transformer, delegatedNode, ctx);
                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return nullptr;
                }

                yson = delegatedNode->GetResult().Content();
            }

            ysonNode = NYT::NodeFromYsonString(yson);
            if (ysonNode.HasKey("FallbackProvider")) {
                nextProvider = ysonNode["FallbackProvider"].AsString();
            } else if (types.QContext.CanWrite()) {
                types.QContext.GetWriter()->Put({EvaluationComponent, key}, yson).GetValueSync();
            }
        } while (ysonNode.HasKey("FallbackProvider"));

        auto dataNode = ysonNode["Data"];
        if (isAtomPipeline) {
            if (isOptionalAtom) {
                if (dataNode.IsEntity() || dataNode.AsList().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to get atom from an empty optional"));
                    return nullptr;
                }

                dataNode = dataNode.AsList().front();
            }
            TString value;
            if (dataNode.IsString()) {
                value = dataNode.AsString();
            } else {
                YQL_ENSURE(dataNode.IsList() && dataNode.AsList().size() == 1 && dataNode.AsList().front().IsString(), "Unexpected atom value: " << NYT::NodeToYsonString(dataNode));
                value = Base64Decode(dataNode.AsList().front().AsString());
            }
            return ctx.NewAtom(node->Pos(), value);
        }

        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TStringStream err;
        TProgramBuilder pgmBuilder(env, functionRegistry);
        TType* mkqlType = NCommon::BuildType(*clonedArg->GetTypeAnn(), pgmBuilder, err);
        if (!mkqlType) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to process type: " << err.Str()));
            return nullptr;
        }

        TMemoryUsageInfo memInfo("Eval");
        THolderFactory holderFactory(alloc.Ref(), memInfo);
        auto value = NCommon::ParseYsonNodeInResultFormat(holderFactory, dataNode, mkqlType, &err);
        if (!value) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Failed to parse data: " << err.Str()));
            return nullptr;
        }

        if (isTypePipeline) {
            auto yson = TStringBuf(value->AsStringRef());
            auto type = NCommon::ParseTypeFromYson(yson, ctx, ctx.GetPosition(node->Pos()));
            if (!type) {
                return nullptr;
            }

            return ExpandType(node->Pos(), *type, ctx);
        }

        if (isCodePipeline) {
            TExprNode::TPtr result = DeserializeGraph(node->Pos(), TStringBuf(value->AsStringRef()), ctx);
            if (!result) {
                return nullptr;
            }

            TNodeOnNodeOwnedMap replaces;
            VisitExpr(*result, [&](const TExprNode& input) {
                if (input.IsCallable("WorldArg")) {
                    YQL_ENSURE(input.ChildrenSize() == 1 && input.Child(0)->IsAtom());
                    auto index = FromString<ui32>(input.Child(0)->Content());
                    YQL_ENSURE(index < marked.ExternalWorldsList.size());
                    YQL_ENSURE(replaces.emplace(&input, marked.ExternalWorldsList[index]).second);
                }

                return true;
            });

            result = ctx.ReplaceNodes(std::move(result), replaces);
            ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
            hasPendingEvaluations = hasPendingEvaluations.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
            return result;
        }

        return NCommon::ValueToExprLiteral(clonedArg->GetTypeAnn(), *value, ctx, node->Pos());
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }

    if (hasPendingEvaluations != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(DEBUG, CoreEval) << "EvaluateExpression - has pending evaluations";
        return hasPendingEvaluations;
    }

    YQL_CLOG(DEBUG, CoreEval) << "EvaluateExpression - finish";
    // repeat some steps
    ctx.Step.Repeat(TExprStep::ValidateProviders);
    ctx.Step.Repeat(TExprStep::Configure);
    ctx.Step.Done(TExprStep::ExprEval);
    return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
}

} // namespace NYql
