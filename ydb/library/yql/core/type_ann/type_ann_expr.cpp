#include "type_ann_expr.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_proposed_by_data.h>
#include <ydb/library/yql/core/yql_opt_rewrite_io.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>

namespace NYql {

namespace {

constexpr bool PrintCallableTimes = false;

class TTypeAnnotationTransformer : public TGraphTransformerBase {
public:
    TTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types)
        : CallableTransformer(callableTransformer)
        , Types(types)
    {
    }

    ~TTypeAnnotationTransformer() {
        if (PrintCallableTimes) {
            std::vector<std::pair<TStringBuf, std::pair<ui64, ui64>>> pairs;
            pairs.reserve(CallableTimes.size());
            for (auto& x : CallableTimes) {
                pairs.emplace_back(x.first, x.second);
            }

            Sort(pairs.begin(), pairs.end(), [](auto a,auto b) { return a.second.first > b.second.first; });
            Cerr << "=============\n";
            for (auto& x : pairs) {
                Cerr << x.first << " : " << CyclesToDuration(x.second.first) << " # " << x.second.second << Endl;
            }
            Cerr << "=============\n";
        }
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoTransform");
        output = input;
        auto status = TransformNode(input, output, ctx);
        UpdateStatusIfChanged(status, input, output);
        if (status.Level != TStatus::Ok) {
            WriteRepeatCallableCount();
        }

        if (status.Level != TStatus::Error && HasRenames) {
            output = ctx.ReplaceNodes(std::move(output), Processed);
        }

        Processed.clear();
        if (status == TStatus::Ok) {
            Types.ExpectedTypes.clear();
            Types.ExpectedColumnOrders.clear();
        }

        HasRenames = false;
        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoGetAsyncFuture");
        Y_UNUSED(input);
        TVector<NThreading::TFuture<void>> futures;
        for (const auto& callable : CallableInputs) {
            futures.push_back(CallableTransformer->GetAsyncFuture(*callable));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoApplyAsyncChanges");
        output = input;
        TStatus combinedStatus = TStatus::Ok;
        for (const auto& callable : CallableInputs) {
            callable->SetState(TExprNode::EState::TypePending);
            TExprNode::TPtr callableOutput;
            auto status = CallableTransformer->ApplyAsyncChanges(callable, callableOutput, ctx);
            Y_ABORT_UNLESS(callableOutput);
            YQL_ENSURE(status != TStatus::Async);
            YQL_ENSURE(callableOutput == callable);
            combinedStatus = combinedStatus.Combine(status);
            if (status.Level == TStatus::Error) {
                callable->SetState(TExprNode::EState::Error);
            }
        }

        CallableInputs.clear();
        if (combinedStatus.Level == TStatus::Ok) {
            Processed.clear();
        }

        return combinedStatus;
    }

    void Rewind() {
        CallableTransformer->Rewind();
        CallableInputs.clear();
        Processed.clear();
        HasRenames = false;
        RepeatCallableCount.clear();
        CurrentFunctions = {};
        CallableTimes.clear();
    }


private:
    void WriteRepeatCallableCount() {
        if (RepeatCallableCount.empty()) {
            return;
        }

        TVector<std::pair<TString, ui64>> values;
        for (const auto& x : RepeatCallableCount) {
            values.push_back({ x.first, x.second });
        }

        Sort(values, [](const std::pair<TString, ui64>& x, const std::pair<TString, ui64>& y) {
            return x.second < y.second;
        });

        TStringStream out;
        out << "Repeated callable stats:\n";
        for (const auto& x : values) {
            out << x.first << "=" << x.second << "\n";
        }

        YQL_CLOG(DEBUG, Core) << out.Str();
        RepeatCallableCount.clear();
    }

    TStatus TransformNode(const TExprNode::TPtr& start, TExprNode::TPtr& output, TExprContext& ctx) {
        output = start;
        auto processedPair = Processed.emplace(start.Get(), nullptr); // by default node is not changed
        if (!processedPair.second) {
            if (processedPair.first->second) {
                output = processedPair.first->second;
                return TStatus::Repeat;
            }

            switch (start->GetState()) {
            case TExprNode::EState::Initial:
                return TStatus(TStatus::Repeat, true);
            case TExprNode::EState::TypeInProgress:
                return IGraphTransformer::TStatus::Async;
            case TExprNode::EState::TypePending:
                if (start->Type() == TExprNode::Lambda) {
                    if (!start->Head().GetTypeAnn()) {
                        return TStatus::Ok;
                    } else if (start->Head().ChildrenSize() == 0) {
                        break;
                    }
                }

                if (start->Type() == TExprNode::Arguments || start->Type() == TExprNode::Argument) {
                    break;
                }

                return TStatus(TStatus::Repeat, true);
            case TExprNode::EState::TypeComplete:
            case TExprNode::EState::ConstrInProgress:
            case TExprNode::EState::ConstrPending:
            case TExprNode::EState::ConstrComplete:
            case TExprNode::EState::ExecutionInProgress:
            case TExprNode::EState::ExecutionRequired:
            case TExprNode::EState::ExecutionPending:
            case TExprNode::EState::ExecutionComplete:
                return TStatus::Ok;
            case TExprNode::EState::Error:
                return TStatus::Error;
            default:
                YQL_ENSURE(false, "Unknown state");
            }
        }

        auto input = start;
        for (size_t transformCount = 0; true; ++transformCount) {
            TIssueScopeGuard issueScope(ctx.IssueManager, [this, input, &ctx]() -> TIssuePtr {
                TStringBuilder str;
                str << "At ";
                switch (input->Type()) {
                case TExprNode::Callable:
                    if (!CurrentFunctions.empty() && CurrentFunctions.top().second) {
                        return nullptr;
                    }

                    if (!CurrentFunctions.empty()) {
                        CurrentFunctions.top().second = true;
                    }

                    str << "function: " << NormalizeCallableName(input->Content());
                    break;
                case TExprNode::List:
                    if (CurrentFunctions.empty()) {
                        str << "tuple";
                    } else if (!CurrentFunctions.top().second) {
                        CurrentFunctions.top().second = true;
                        str << "function: " << CurrentFunctions.top().first;
                    } else {
                        return nullptr;
                    }
                    break;
                case TExprNode::Lambda:
                    if (CurrentFunctions.empty()) {
                        str << "lambda";
                    } else if (!CurrentFunctions.top().second) {
                        CurrentFunctions.top().second = true;
                        str << "function: " << CurrentFunctions.top().first;
                    } else {
                        return nullptr;
                    }
                    break;
                default:
                    str << "unknown";
                }

                return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()), str);
            });

            if (input->Type() == TExprNode::Callable) {
                CurrentFunctions.push(std::make_pair(input->Content(), false));
            }
            Y_SCOPE_EXIT(this, input) {
                if (input->Type() == TExprNode::Callable) {
                    CurrentFunctions.pop();
                    if (!CurrentFunctions.empty() && CurrentFunctions.top().first.EndsWith('!')) {
                        CurrentFunctions.top().second = true;
                    }
                }
            };

            TStatus retStatus = TStatus::Error;
            switch (input->GetState()) {
            case TExprNode::EState::Initial:
                break;
            case TExprNode::EState::TypeInProgress:
                return IGraphTransformer::TStatus::Async;
            case TExprNode::EState::TypePending:
                break;
            case TExprNode::EState::TypeComplete:
            case TExprNode::EState::ConstrInProgress:
            case TExprNode::EState::ConstrPending:
            case TExprNode::EState::ConstrComplete:
            case TExprNode::EState::ExecutionInProgress:
            case TExprNode::EState::ExecutionRequired:
            case TExprNode::EState::ExecutionPending:
            case TExprNode::EState::ExecutionComplete:
                return TStatus::Ok;
            case TExprNode::EState::Error:
                return TStatus::Error;
            default:
                YQL_ENSURE(false, "Unknown state");
            }

            if (transformCount >= ctx.TypeAnnNodeRepeatLimit) {
                TConvertToAstSettings settings;
                settings.AllowFreeArgs = true;
                auto ast = ConvertToAst(*input, ctx, settings);
                if (ast.Root) {
                    TStringStream s;
                    ast.Root->PrettyPrintTo(s, TAstPrintFlags::ShortQuote | TAstPrintFlags::AdaptArbitraryContent | TAstPrintFlags::PerLine);
                    YQL_CLOG(INFO, Core) << "Too many transformations for node:\n" << s.Str();
                }
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()),
                    TStringBuilder() << "YQL: Internal core error! Type annotation of node " << input->Content()
                        << " with type " << input->Type() << " takes too much iterations: "
                        << ctx.TypeAnnNodeRepeatLimit << ". You may set TypeAnnNodeRepeatLimit as flags for config provider."));
                return TStatus::Error;
            }

            input->SetState(TExprNode::EState::TypePending);
            switch (input->Type()) {
            case TExprNode::Atom:
            {
                input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
                CheckExpected(*input, ctx);
                return TStatus::Ok;
            }

            case TExprNode::List:
            {
                TStatus combinedStatus = TStatus::Ok;
                TExprNode::TListType newChildren;
                newChildren.reserve(input->ChildrenSize());
                bool updatedChildren = false;
                for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
                    const auto child = input->ChildPtr(i);
                    TExprNode::TPtr newChild;
                    auto childStatus = TransformNode(child, newChild, ctx);
                    UpdateStatusIfChanged(childStatus, child, newChild);
                    updatedChildren = updatedChildren || (newChild != child);
                    combinedStatus = combinedStatus.Combine(childStatus);
                    newChildren.emplace_back(std::move(newChild));
                }

                if (combinedStatus != TStatus::Ok) {
                    if (combinedStatus.Level == TStatus::Error) {
                        input->SetState(TExprNode::EState::Error);
                    }
                    else if (updatedChildren) {
                        input->ChangeChildrenInplace(std::move(newChildren));
                    }

                    retStatus = combinedStatus;
                    break;
                }

                TTypeAnnotationNode::TListType children;
                children.reserve(input->ChildrenSize());
                bool isUnit = false;
                for (auto& child : input->Children()) {
                    if (!EnsureComposable(*child, ctx)) {
                        input->SetState(TExprNode::EState::Error);
                        return TStatus::Error;
                    }

                    children.push_back(child->GetTypeAnn());
                    if (child->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Unit) {
                        isUnit = true;
                    }
                }

                input->SetTypeAnn(isUnit ?
                    (const TTypeAnnotationNode*)ctx.MakeType<TUnitExprType>() :
                    ctx.MakeType<TTupleExprType>(children));
                CheckExpected(*input, ctx);
                return TStatus::Ok;
            }

            case TExprNode::Lambda:
            {
                YQL_ENSURE(input->ChildrenSize() > 0U);
                const auto& args = input->Head();
                for (ui32 i = 0; i < args.ChildrenSize(); ++i) {
                    args.Child(i)->SetArgIndex(i);
                }

                TExprNode::TPtr out;
                auto argStatus = TransformNode(input->HeadPtr(), out, ctx);
                UpdateStatusIfChanged(argStatus, input->HeadPtr(), out);
                if (argStatus.Level == TStatus::Error) {
                    input->SetState(TExprNode::EState::Error);
                    return argStatus;
                }

                if (argStatus.Level == TStatus::Repeat)
                    return TStatus::Ok;

                TStatus combinedStatus = TStatus::Ok;
                TExprNode::TListType newChildren;
                newChildren.reserve(input->ChildrenSize());
                newChildren.emplace_back(input->HeadPtr());
                bool updatedChildren = false;
                for (ui32 i = 1U; i < input->ChildrenSize(); ++i) {
                    const auto child = input->ChildPtr(i);
                    TExprNode::TPtr newChild;
                    auto childStatus = TransformNode(child, newChild, ctx);
                    UpdateStatusIfChanged(childStatus, child, newChild);
                    updatedChildren = updatedChildren || (newChild != child);
                    combinedStatus = combinedStatus.Combine(childStatus);
                    newChildren.emplace_back(std::move(newChild));
                }

                if (combinedStatus != TStatus::Ok) {
                    if (combinedStatus.Level == TStatus::Error) {
                        input->SetState(TExprNode::EState::Error);
                    }
                    else if (updatedChildren) {
                        input->ChangeChildrenInplace(std::move(newChildren));
                    }

                    retStatus = combinedStatus;
                    break;
                }

                if (newChildren.size() != 2U) {
                    TTypeAnnotationNode::TListType rootTypes;
                    rootTypes.reserve(newChildren.size() - 1U);
                    for (ui32 i = 1U; i < newChildren.size(); ++i) {
                        rootTypes.emplace_back(newChildren[i]->GetTypeAnn());
                    }
                    input->SetTypeAnn(ctx.MakeType<TMultiExprType>(rootTypes));
                } else {
                    input->SetTypeAnn(input->Tail().GetTypeAnn());
                }

                if (input->GetTypeAnn()) {
                    CheckExpected(*input, ctx);
                }

                return TStatus::Ok;
            }

            case TExprNode::Argument:
                if (input->GetTypeAnn()) {
                    if (input->Type() == TExprNode::Lambda) {
                        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Unable to use lambda as argument"));
                        input->SetState(TExprNode::EState::Error);
                        return TStatus::Error;
                    }
                }
                else {
                    return TStatus::Repeat;
                }

                return TStatus::Ok;

            case TExprNode::Callable:
            {
                TStatus combinedStatus = TStatus::Ok;
                {
                    TExprNode::TListType newChildren;
                    newChildren.reserve(input->ChildrenSize());
                    bool updatedChildren = false;
                    for (ui32 i = 0; i < input->ChildrenSize(); ++i) {
                        const auto child = input->ChildPtr(i);
                        TExprNode::TPtr newChild;
                        auto childStatus = TransformNode(child, newChild, ctx);
                        UpdateStatusIfChanged(childStatus, child, newChild);
                        updatedChildren = updatedChildren || (newChild != child);
                        combinedStatus = combinedStatus.Combine(childStatus);
                        newChildren.emplace_back(std::move(newChild));
                    }

                    if (combinedStatus != TStatus::Ok) {
                        if (combinedStatus.Level == TStatus::Error) {
                            input->SetState(TExprNode::EState::Error);
                        }
                        else if (updatedChildren) {
                            input->ChangeChildrenInplace(std::move(newChildren));
                        }

                        retStatus = combinedStatus;
                        break;
                    }
                }

                CurrentFunctions.top().second = true;
                auto cyclesBefore = PrintCallableTimes ? GetCycleCount() : 0;
                auto status = CallableTransformer->Transform(input, output, ctx);
                auto cyclesAfter = PrintCallableTimes ? GetCycleCount() : 0;
                if (PrintCallableTimes) {
                    auto& x = CallableTimes[input->Content()];
                    x.first += (cyclesAfter - cyclesBefore);
                    ++x.second;
                }

                if (status == TStatus::Error) {
                    input->SetState(TExprNode::EState::Error);
                    return status;
                }

                if (status == TStatus::Ok) {
                    if (!input->GetTypeAnn()) {
                        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Node is not annotated yet"));
                        input->SetState(TExprNode::EState::Error);
                        return TStatus::Error;
                    }

                    input->SetState(TExprNode::EState::TypeComplete);
                    CheckExpected(*input, ctx);
                }
                else if (status == TStatus::Async) {
                    CallableInputs.push_back(input);
                    input->SetState(TExprNode::EState::TypeInProgress);
                } else {
                    RepeatCallableCount[input.Get()->Content()] += 1;
                    if (output != input.Get()) {
                        processedPair.first->second = output;
                        HasRenames = true;
                    }

                    retStatus = status;
                    break;
                }

                return status;
            }

            case TExprNode::World:
            {
                input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
                CheckExpected(*input, ctx);
                return TStatus::Ok;
            }

            case TExprNode::Arguments:
            {
                if (input->Children().empty()) {
                    if (input->GetTypeAnn()) {
                        input->SetState(TExprNode::EState::TypeComplete);
                        return TStatus::Ok;
                    }

                    return TStatus::Repeat;
                }

                TStatus combinedStatus = TStatus::Ok;
                for (auto& child : input->Children()) {
                    TExprNode::TPtr tmp;
                    auto childStatus = TransformNode(child, tmp, ctx);
                    UpdateStatusIfChanged(childStatus, child, tmp);
                    YQL_ENSURE(tmp == child);
                    combinedStatus = combinedStatus.Combine(childStatus);
                }

                if (combinedStatus != TStatus::Ok) {
                    if (combinedStatus.Level == TStatus::Error) {
                        input->SetState(TExprNode::EState::Error);
                    }

                    return combinedStatus;
                }

                input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
                return TStatus::Ok;
            }

            default:
                YQL_ENSURE(false, "Unknown type");
            }

            if (retStatus.Level != TStatus::Repeat || retStatus.HasRestart) {
                return retStatus;
            }

            input = output;
        }
    }

    void UpdateStatusIfChanged(TStatus& status, const TExprNode::TPtr& input, const TExprNode::TPtr& output) {
        if (status.Level == TStatus::Ok && input != output) {
            status = TStatus(TStatus::Repeat, status.HasRestart);
        }
    }

    void CheckExpected(const TExprNode& input, TExprContext& ctx) {
        CheckExpectedTypeAndColumnOrder(input, ctx, Types);
    }

private:
    TAutoPtr<IGraphTransformer> CallableTransformer;
    TTypeAnnotationContext& Types;
    TDeque<TExprNode::TPtr> CallableInputs;
    TNodeOnNodeOwnedMap Processed;
    bool HasRenames = false;
    THashMap<TString, ui64> RepeatCallableCount;
    TStack<std::pair<TStringBuf, bool>> CurrentFunctions;
    THashMap<TStringBuf, std::pair<ui64, ui64>> CallableTimes;
};

} // namespace

IGraphTransformer::TStatus CheckWholeProgramType(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    if (input->Type() == TExprNode::Lambda || input->GetTypeAnn()->GetKind() != ETypeAnnotationKind::World) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Return must be world"));
        input->SetState(TExprNode::EState::Error);
        return IGraphTransformer::TStatus::Error;
    }

    return IGraphTransformer::TStatus::Ok;
}

TAutoPtr<IGraphTransformer> CreateTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer,
    TTypeAnnotationContext& types) {
    return new TTypeAnnotationTransformer(callableTransformer, types);
}

TAutoPtr<IGraphTransformer> CreateFullTypeAnnotationTransformer(
        bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext)
{
    TVector<TTransformStage> transformers;
    auto issueCode = TIssuesIds::CORE_PRE_TYPE_ANN;
    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(&ExpandApply),
        "ExpandApply",
        issueCode));
    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(
            [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return ValidateProviders(input, output, ctx, typeAnnotationContext);
        }),
        "ValidateProviders",
        issueCode));

    transformers.push_back(TTransformStage(
        CreateConfigureTransformer(typeAnnotationContext),
        "Configure",
        issueCode));

    // NOTE: add fake EvaluateExpression step to break infinite loop
    // (created by Repeat on ExprEval step after RewriteIO completion)
    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(
            [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                output = input;
                ctx.Step.Done(TExprStep::ExprEval);
                return IGraphTransformer::TStatus::Ok;
            }
        ),
        "EvaluateExpression",
        issueCode));

    transformers.push_back(TTransformStage(
        CreateIODiscoveryTransformer(typeAnnotationContext),
        "IODiscovery",
        issueCode));
    transformers.push_back(TTransformStage(
        CreateEpochsTransformer(typeAnnotationContext),
        "Epochs",
        issueCode));

    transformers.push_back(TTransformStage(
        CreateIntentDeterminationTransformer(typeAnnotationContext),
        "IntentDetermination",
        issueCode));
    transformers.push_back(TTransformStage(
        CreateTableMetadataLoader(typeAnnotationContext),
        "TableMetadataLoader",
        issueCode));
    auto& typeCtx = typeAnnotationContext;
    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(
            [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return RewriteIO(input, output, typeCtx, ctx);
        }),
        "RewriteIO",
        issueCode));

    issueCode = TIssuesIds::CORE_TYPE_ANN;
    auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(typeAnnotationContext);
    auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, typeAnnotationContext);
    transformers.push_back(TTransformStage(
        typeTransformer,
        "TypeAnnotation",
        issueCode));
    if (wholeProgram) {
        transformers.push_back(TTransformStage(
            CreateFunctorTransformer(&CheckWholeProgramType),
            "CheckWholeProgramType",
            issueCode));
    }

    return CreateCompositeGraphTransformer(transformers, true);
}

bool SyncAnnotateTypes(
        TExprNode::TPtr& root, TExprContext& ctx, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext)
{
    auto fullTransformer = CreateFullTypeAnnotationTransformer(wholeProgram, typeAnnotationContext);
    return SyncTransform(*fullTransformer, root, ctx) == IGraphTransformer::TStatus::Ok;
}

bool InstantAnnotateTypes(
        TExprNode::TPtr& root, TExprContext& ctx, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext)
{
    auto fullTransformer = CreateFullTypeAnnotationTransformer(wholeProgram, typeAnnotationContext);
    return InstantTransform(*fullTransformer, root, ctx) == IGraphTransformer::TStatus::Ok;
}

TExprNode::TPtr ParseAndAnnotate(
        const TStringBuf& str,
        TExprContext& exprCtx, bool instant, bool wholeProgram,
        TTypeAnnotationContext& typeAnnotationContext)
{
    TAstParseResult astRes = ParseAst(str);
    exprCtx.IssueManager.AddIssues(astRes.Issues);
    if (!astRes.IsOk()) {
        return nullptr;
    }

    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr)) {
        return nullptr;
    }

    const bool annotated = instant
            ? InstantAnnotateTypes(exprRoot, exprCtx, wholeProgram, typeAnnotationContext)
            : SyncAnnotateTypes(exprRoot, exprCtx, wholeProgram, typeAnnotationContext);
    if (!annotated) {
        return nullptr;
    }

    return exprRoot;
}

} // namespace NYql
