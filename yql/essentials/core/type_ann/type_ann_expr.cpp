#include "type_ann_expr.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/yql_opt_proposed_by_data.h>
#include <yql/essentials/core/yql_opt_rewrite_io.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_func_stack.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/utils/exception_utils.h>
#include <yql/essentials/utils/log/log.h>

#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>

namespace NYql {

namespace {

constexpr bool PrintCallableTimes = false;

class TTypeAnnotationTransformer : public TGraphTransformerBase {
public:
    TTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types,
        ETypeCheckMode mode)
        : CallableTransformer_(callableTransformer)
        , Types_(types)
        , Mode_(mode)
    {
    }

    ~TTypeAnnotationTransformer() override {
        if (!PrintCallableTimes) {
            return;
        }
        NYql::WithAbortOnException([&] {
            std::vector<std::pair<TStringBuf, std::pair<ui64, ui64>>> pairs;
            pairs.reserve(CallableTimes_.size());
            for (auto& x : CallableTimes_) {
                pairs.emplace_back(x.first, x.second);
            }

            Sort(pairs.begin(), pairs.end(), [](auto a, auto b) { return a.second.first > b.second.first; });
            Cerr << "=============\n";
            for (auto& x : pairs) {
                Cerr << x.first << " : " << CyclesToDuration(x.second.first) << " # " << x.second.second << Endl;
            }
            Cerr << "=============\n";
        }, "TTypeAnnotationTransformer");
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoTransform");
        output = input;
        if (Mode_ == ETypeCheckMode::Initial && IsComplete_) {
            return TStatus::Ok;
        }

        if (IsOptimizerEnabled<KeepWorldOptName>(Types_) && !IsOptimizerDisabled<KeepWorldOptName>(Types_)) {
            KeepWorldEnabled_ = true;
        }

        auto status = TransformNode(input, output, ctx);
        UpdateStatusIfChanged(status, input, output);
        if (status.Level != TStatus::Ok) {
            WriteRepeatCallableCount();
        }

        if (status.Level != TStatus::Error && HasRenames_) {
            output = ctx.ReplaceNodes(std::move(output), Processed_);
        }

        Processed_.clear();
        if (status == TStatus::Ok) {
            Types_.ExpectedTypes.clear();
            Types_.ExpectedColumnOrders.clear();
        }

        HasRenames_ = false;
        if (Mode_ == ETypeCheckMode::Initial && status == TStatus::Ok) {
            IsComplete_ = true;
        }

        if (Mode_ == ETypeCheckMode::Repeat) {
            CheckFatalTypeError(status);
        }

        return status;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoGetAsyncFuture");
        Y_UNUSED(input);
        TVector<NThreading::TFuture<void>> futures;
        for (const auto& callable : CallableInputs_) {
            futures.push_back(CallableTransformer_->GetAsyncFuture(*callable));
        }

        return WaitExceptionOrAll(futures);
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_PROFILE_SCOPE(DEBUG, "TypeAnnotationTransformer::DoApplyAsyncChanges");
        output = input;
        TStatus combinedStatus = TStatus::Ok;
        for (const auto& callable : CallableInputs_) {
            callable->SetState(TExprNode::EState::TypePending);
            TExprNode::TPtr callableOutput;
            auto status = CallableTransformer_->ApplyAsyncChanges(callable, callableOutput, ctx);
            Y_ABORT_UNLESS(callableOutput);
            YQL_ENSURE(status != TStatus::Async);
            YQL_ENSURE(callableOutput == callable);
            combinedStatus = combinedStatus.Combine(status);
            if (status.Level == TStatus::Error) {
                callable->SetState(TExprNode::EState::Error);
            }
        }

        CallableInputs_.clear();
        if (combinedStatus.Level == TStatus::Ok) {
            Processed_.clear();
        }

        if (Mode_ == ETypeCheckMode::Repeat) {
            CheckFatalTypeError(combinedStatus);
        }

        return combinedStatus;
    }

    void Rewind() override {
        CallableTransformer_->Rewind();
        CallableInputs_.clear();
        Processed_.clear();
        HasRenames_ = false;
        RepeatCallableCount_.clear();
        FunctionStack_.Reset();
        CallableTimes_.clear();
        IsComplete_ = false;
    }


private:
    void WriteRepeatCallableCount() {
        if (RepeatCallableCount_.empty()) {
            return;
        }

        TVector<std::pair<TString, ui64>> values;
        for (const auto& x : RepeatCallableCount_) {
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
        RepeatCallableCount_.clear();
    }

    TStatus TransformNode(const TExprNode::TPtr& start, TExprNode::TPtr& output, TExprContext& ctx) {
        output = start;
        auto processedPair = Processed_.emplace(start.Get(), nullptr); // by default node is not changed
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
            FunctionStack_.EnterFrame(*input, ctx);
            Y_DEFER {
                FunctionStack_.LeaveFrame(*input, ctx);
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
                CalculateWorld(*input);
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
                CalculateWorld(*input);
                input->UpdateSideEffectsFromChildren();
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
                    CalculateWorld(*input);
                    input->UpdateSideEffectsFromChildren();
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

                FunctionStack_.MarkUsed();
                input->UpdateSideEffectsFromChildren();
                auto cyclesBefore = PrintCallableTimes ? GetCycleCount() : 0;
                auto status = DoCallableTransform(input, output, ctx);
                auto cyclesAfter = PrintCallableTimes ? GetCycleCount() : 0;
                if (PrintCallableTimes) {
                    auto& x = CallableTimes_[input->Content()];
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
                    CalculateWorld(*input);
                    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::World) {
                        input->SetSideEffects(ESideEffects::None);
                    }
                }
                else if (status == TStatus::Async) {
                    CallableInputs_.push_back(input);
                    input->SetState(TExprNode::EState::TypeInProgress);
                } else {
                    RepeatCallableCount_[input.Get()->Content()] += 1;
                    if (output != input.Get()) {
                        processedPair.first->second = output;
                        HasRenames_ = true;
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
                CalculateWorld(*input);
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
        CheckExpectedTypeAndColumnOrder(input, ctx, Types_);
    }

    void CalculateWorld(TExprNode& input) {
        if (!KeepWorldEnabled_) {
            return;
        }

        YQL_ENSURE(!input.GetWorldLinks());
        if (input.IsAtom() || input.IsWorld() || input.IsArgument()) {
            return;
        }

        TExprNode::TListType candidates;
        bool hasWorlds = false;
        for (const auto& child : input.Children()) {
            if (!child->GetTypeAnn()) {
                continue;
            }

            if (child->GetTypeAnn()->ReturnsWorld()) {
                if (!child->IsWorld()) {
                    hasWorlds = true;
                    candidates.push_back(child);
                }
            } else {
                auto inner = child->GetWorldLinks();
                if (inner) {
                    candidates.insert(candidates.end(), inner->begin(), inner->end());
                }
            }
        }

        SortUniqueBy(candidates, [](const auto& p){ return p->UniqueId(); });
        if (!candidates.empty()) {
            if (!hasWorlds) {
                for (const auto& child : input.Children()) {
                    if (!child->GetTypeAnn()) {
                        continue;
                    }

                    if (!child->GetTypeAnn()->ReturnsWorld()) {
                        auto inner = child->GetWorldLinks();
                        if (inner && *inner == candidates) {
                            input.SetWorldLinks(std::move(inner));
                            return;
                        }
                    }
                }
            }

            input.SetWorldLinks(std::make_shared<TExprNode::TListType>(std::move(candidates)));
        }
    }

protected:
    virtual IGraphTransformer::TStatus DoCallableTransform(const TExprNode::TPtr& input,
        TExprNode::TPtr& output, TExprContext& ctx) {
        return CallableTransformer_->Transform(input, output, ctx);
    }

    TTypeAnnotationContext& GetTypes() {
        return Types_;
    }

private:
    TAutoPtr<IGraphTransformer> CallableTransformer_;
    TTypeAnnotationContext& Types_;
    const ETypeCheckMode Mode_;
    bool IsComplete_ = false;
    TDeque<TExprNode::TPtr> CallableInputs_;
    TNodeOnNodeOwnedMap Processed_;
    bool HasRenames_ = false;
    THashMap<TString, ui64> RepeatCallableCount_;
    TFunctionStack FunctionStack_;
    THashMap<TStringBuf, std::pair<ui64, ui64>> CallableTimes_;
    bool KeepWorldEnabled_ = false;
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
    TTypeAnnotationContext& types, ETypeCheckMode mode) {
    return new TTypeAnnotationTransformer(callableTransformer, types, mode);
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
            }
        ),
        "ValidateProviders",
        issueCode));
    transformers.push_back(TTransformStage(
        CreateConfigureTransformer(typeAnnotationContext),
        "Configure",
        issueCode));
    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(
            [&typeAnnotationContext](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                return ExpandSeq(input, output, ctx, typeAnnotationContext);
            }
        ),
        "ExpandSeq",
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
    auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, typeAnnotationContext, ETypeCheckMode::Single);
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

class TPartialTypeAnnotationTransformer : public TTypeAnnotationTransformer {
    using TBase = TTypeAnnotationTransformer;
public:
    TPartialTypeAnnotationTransformer(TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types)
        : TBase(callableTransformer, types, ETypeCheckMode::Initial)
    {
    }

    IGraphTransformer::TStatus DoCallableTransform(const TExprNode::TPtr& input,
        TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (input->IsCallable("Configure!") && input->Child(1)->Head().Content() == ConfigProviderName) {
            auto ptr = GetTypes().DataSourceMap.FindPtr(ConfigProviderName);
            YQL_ENSURE(ptr);
            auto status = (*ptr)->GetConfigurationTransformer().Transform(input, output, ctx);
            if (status == IGraphTransformer::TStatus::Ok) {
                input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
            }

            return status;
        }

        if (input->IsCallable({"Commit!", "CommitAll!", "Write!", "Configure!"})) {
            input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (input->IsCallable({"MrTableConcat", "MrTableRange",
            "MrTableConcatStrict", "MrTableRangeStrict", "TempTable", "MrFolder",
            "MrTableEach", "MrTableEachStrict", "MrPartitions", "MrPartitionsStrict",
            "MrPartitionList", "MrPartitionListStrict", "MrWalkFolders"})) {
            input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (input->IsCallable("Read!")) {
            TTypeAnnotationNode::TListType children;
            children.push_back(ctx.MakeType<TWorldExprType>());
            children.push_back(ctx.MakeType<TListExprType>(ctx.MakeType<TUniversalStructExprType>()));
            input->SetTypeAnn(ctx.MakeType<TTupleExprType>(children));
            return IGraphTransformer::TStatus::Ok;
        }

        if (input->IsCallable({"Udf", "ScriptUdf", "EvaluateAtom",
            "EvaluateExpr", "EvaluateType", "EvaluateCode", "QuoteCode", "Parameter",
            "SubqueryOrderBy", "SubqueryAssumeOrderBy", "SubqueryExtendFor", "SubqueryUnionAllFor",
            "SubqueryMergeFor", "SubqueryUnionMergeFor",
            "SubqueryExtend","SubqueryUnionAll", "SubqueryMerge", "SubqueryUnionMerge",
            "EvaluateFor!", "EvaluateParallelFor!", "EvaluateIf!"})) {
            input->SetTypeAnn(ctx.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (input->IsCallable({"FileContent","FilePath","FolderPath", "TableName",
            "SecureParam", "TablePath"})) {
            input->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String));
            return IGraphTransformer::TStatus::Ok;
        }

        return TBase::DoCallableTransform(input, output, ctx);
    }

private:
};

TAutoPtr<IGraphTransformer> CreatePartialTypeAnnotationTransformer(
    TAutoPtr<IGraphTransformer> callableTransformer, TTypeAnnotationContext& types) {
    return new TPartialTypeAnnotationTransformer(callableTransformer, types);
}

namespace {

class TFakeArrowResolver : public IArrowResolver {
public:
    EStatus LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode* returnType, TExprContext& ctx) const override {
        Y_UNUSED(pos);
        Y_UNUSED(name);
        Y_UNUSED(argTypes);
        Y_UNUSED(returnType);
        Y_UNUSED(ctx);
        return EStatus::OK;
    }

    EStatus HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, TExprContext& ctx) const override {
        Y_UNUSED(pos);
        Y_UNUSED(from);
        Y_UNUSED(to);
        Y_UNUSED(ctx);
        return EStatus::OK;
    }

    EStatus AreTypesSupported(const TPosition& pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx,
        const TUnsupportedTypeCallback& onUnsupported = {}) const final {
        Y_UNUSED(pos);
        Y_UNUSED(types);
        Y_UNUSED(ctx);
        Y_UNUSED(onUnsupported);
        return EStatus::OK;
    }
};

class TFakeLayersRegistry : public NLayers::ILayersRegistry {
public:
    TMaybe<TVector<NLayers::TKey>> ResolveLogicalLayers(const TVector<NLayers::TLayerOrder>& orders, TExprContext& ctx) const final {
        Y_UNUSED(orders);
        Y_UNUSED(ctx);
        return Nothing();
    }

    TMaybe<NLayers::TLocations> ResolveLayers(const TVector<NLayers::TKey>& order, const TString& system, const TString& cluster, TExprContext& ctx) const final {
        Y_UNUSED(order);
        Y_UNUSED(system);
        Y_UNUSED(cluster);
        Y_UNUSED(ctx);
        return Nothing();
    }

    bool HasLayer(const NLayers::TKey& key) const override {
        Y_UNUSED(key);
        return false;
    }

    bool AddLayer(const TString& name, const TMaybe<TString>& parent, const TMaybe<TString>& url, TExprContext& ctx) override {
        Y_UNUSED(name);
        Y_UNUSED(parent);
        Y_UNUSED(url);
        Y_UNUSED(ctx);
        return true;
    }

    bool AddLayerFromJson(TStringBuf json, TExprContext& ctx) final {
        Y_UNUSED(json);
        Y_UNUSED(ctx);
        return true;
    }

    void ClearLayers() final {
    }
};

}

bool PartialAnnonateTypes(TAstNode* astRoot, TLangVersion langver, TIssues& issues,
    std::function<TIntrusivePtr<IDataProvider>(TTypeAnnotationContext&)> configProviderFactory) {
    YQL_ENSURE(astRoot, "AST root is null");

    TExprContext ctx;
    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*astRoot, exprRoot, ctx, /* resolver= */ nullptr, /* urlListerManager */ nullptr,
                        /* hasAnnotations= */ false, /* typeAnnotationIndex= */ Max<ui32>(), /* syntaxVersion= */ 1)) {
        issues.AddIssues(ctx.IssueManager.GetCompletedIssues());
        return false;
    }

    TTypeAnnotationContext typeCtx;
    typeCtx.LangVer = langver;
    typeCtx.ArrowResolver = new TFakeArrowResolver;
    typeCtx.LayersRegistry = new TFakeLayersRegistry;
    typeCtx.UserDataStorage = new TUserDataStorage(nullptr, {}, nullptr, new TUdfIndex);
    auto configProvder = configProviderFactory(typeCtx);
    typeCtx.AddDataSource(ConfigProviderName, configProvder);
    auto callableTypeAnnTransformer = CreateExtCallableTypeAnnotationTransformer(typeCtx);
    TVector<TTransformStage> transformers;
    transformers.push_back(TTransformStage(CreateFunctorTransformer(&ExpandApply),
                                            "ExpandApply", TIssuesIds::CORE_PRE_TYPE_ANN));
    transformers.push_back(TTransformStage(CreateFunctorTransformer([&typeCtx](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx){
        TOptimizeExprSettings settings(&typeCtx);
        return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext& ctx){
            if (node->IsCallable("FormatCode")) {
                return ctx.Builder(node->Pos())
                    .Callable("String")
                        .Atom(0, "")
                    .Seal()
                    .Build();
            }

            return node;
        }, ctx, settings);
    }),
                                            "RewriteEvaluation", TIssuesIds::CORE_PRE_TYPE_ANN));
    transformers.push_back(TTransformStage(
        CreatePartialTypeAnnotationTransformer(std::move(callableTypeAnnTransformer), typeCtx),
        "PartialTypeAnn", TIssuesIds::CORE_PARTIAL_TYPE_ANN));
    auto transformer = CreateCompositeGraphTransformer(transformers, /* useIssueScopes= */ true);
    auto status = InstantTransform(*transformer, exprRoot, ctx);
    issues.AddIssues(ctx.IssueManager.GetCompletedIssues());
    return status == IGraphTransformer::TStatus::Ok;
}

void CheckFatalTypeError(IGraphTransformer::TStatus status) {
    if (status == IGraphTransformer::TStatus::Error) {
        throw yexception() << "Detected a type error after initial validation";
    }
}

} // namespace NYql
