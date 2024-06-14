#include "yql_result_provider.h"

#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/ast/yql_gc_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/cast.h>

namespace NYql {

namespace {
    using namespace NKikimr;
    using namespace NKikimr::NMiniKQL;
    using namespace NNodes;

    class TYsonResultWriter : public IResultWriter
    {
    public:
        TYsonResultWriter(NYson::EYsonFormat format)
            : Writer(new NYson::TYsonWriter(&PartialStream, format, ::NYson::EYsonType::Node, true))
        {}

        void Init(bool discard, const TString& label, TMaybe<TPosition> pos, bool unordered) override {
            Discard = discard;
            Unordered = unordered;
            if (!Discard) {
                Writer->OnBeginMap();
                if (label) {
                    Writer->OnKeyedItem("Label");
                    Writer->OnStringScalar(label);
                }

                if (pos) {
                    Writer->OnKeyedItem("Position");
                    Writer->OnBeginMap();
                    Writer->OnKeyedItem("File");
                    Writer->OnStringScalar(pos->File ? pos->File : "<main>");
                    Writer->OnKeyedItem("Row");
                    Writer->OnInt64Scalar(pos->Row);
                    Writer->OnKeyedItem("Column");
                    Writer->OnInt64Scalar(pos->Column);
                    Writer->OnEndMap();
                }

                Writer->OnKeyedItem("Write");
                Writer->OnBeginList();
            }
        }

        void Write(const TStringBuf& resultData) override {
            if (!Discard) {
                Writer->OnListItem();
                Writer->OnRaw(resultData);
            }
        }

        void Commit(bool overflow) override {
            if (!Discard) {
                Writer->OnEndList();
                if (overflow) {
                    Writer->OnKeyedItem("Truncated");
                    Writer->OnBooleanScalar(true);
                }
                if (Unordered) {
                    Writer->OnKeyedItem("Unordered");
                    Writer->OnBooleanScalar(true);
                }
                Writer->OnEndMap();
            }
        }

        bool IsDiscard() const override {
            return Discard;
        }

        TStringBuf Str() override {
            return PartialStream.Str();
        }

        ui64 Size() override {
            return PartialStream.Size();
        }

    private:
        TStringStream PartialStream;
        TAutoPtr<NYson::TYsonWriter> Writer;
        bool Discard = false;
        bool Unordered = false;
    };

    IGraphTransformer::TStatus ValidateColumns(TExprNode::TPtr& columns, const TTypeAnnotationNode* listType, TExprContext& ctx) {
        bool hasPrefixes = false;
        bool hasAutoNames = false;
        for (auto& child : columns->Children()) {
            if (HasError(child->GetTypeAnn(), ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!child->IsAtom() && !child->IsList()) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), "either atom or tuple is expected"));
                return IGraphTransformer::TStatus::Error;
            }

            if (child->IsList()) {
                if (!EnsureTupleMinSize(*child, 1, ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(*child->Child(0), ctx)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (child->Child(0)->Content() == "prefix") {
                    if (!EnsureTupleSize(*child, 2, ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    if (!EnsureAtom(*child->Child(1), ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    hasPrefixes = true;
                } else if (child->Child(0)->Content() == "auto") {
                    if (!EnsureTupleSize(*child, 1, ctx)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    hasAutoNames = true;
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() <<
                        "Expected 'prefix' or 'auto', but got: " << child->Child(0)->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (listType->GetKind() == ETypeAnnotationKind::EmptyList) {
            return IGraphTransformer::TStatus::Ok;
        }

        if (listType->GetKind() != ETypeAnnotationKind::List) {
            ctx.AddError(TIssue(ctx.GetPosition(columns->Pos()), "columns requires list of struct"));
            return IGraphTransformer::TStatus::Error;
        }

        auto itemType = listType->Cast<TListExprType>()->GetItemType();
        if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
            ctx.AddError(TIssue(ctx.GetPosition(columns->Pos()), "columns requires list of struct"));
            return IGraphTransformer::TStatus::Error;
        }

        auto structType = itemType->Cast<TStructExprType>();
        TSet<TString> usedFields;
        TExprNode::TListType orderedFields;
        for (size_t i = 0; i < columns->ChildrenSize(); ++i) {
            auto child = columns->ChildPtr(i);
            if (child->IsAtom()) {
                orderedFields.push_back(child);
                if (!structType->FindItem(child->Content())) {
                    if (hasAutoNames) {
                        columns = {};
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                    }
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() <<
                        "Unknown field in hint: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!usedFields.insert(TString(child->Content())).second) {
                    if (hasAutoNames) {
                        columns = {};
                        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                    }
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() <<
                        "Duplicate field in hint: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
            } else if (child->Child(0)->Content() == "auto") {
                TString columnName = "column" + ToString(i);
                if (!structType->FindItem(columnName) || !usedFields.insert(columnName).second) {
                    columns = {};
                    return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                }
                orderedFields.push_back(ctx.NewAtom(child->Pos(), columnName));
            } else {
                auto prefix = child->Child(1)->Content();
                for (auto& x : structType->GetItems()) {
                    if (x->GetName().StartsWith(prefix)) {
                        orderedFields.push_back(ctx.NewAtom(child->Pos(), x->GetName()));
                        if (!usedFields.insert(TString(x->GetName())).second) {
                            if (hasAutoNames) {
                                columns = {};
                                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
                            }
                            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() <<
                                "Duplicate field in hint: " << x->GetName()));
                            return IGraphTransformer::TStatus::Error;
                        }
                    }
                }
            }
        }

        if (usedFields.size() != structType->GetSize()) {
            if (hasAutoNames) {
                columns = {};
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
            }
            ctx.AddError(TIssue(ctx.GetPosition(columns->Pos()), TStringBuilder() <<
                "Mismatch of fields in hint and in the struct, columns fields: " << usedFields.size()
                << ", struct fields:" << structType->GetSize()));
            return IGraphTransformer::TStatus::Error;
        }

        if (hasPrefixes || hasAutoNames) {
            columns = ctx.NewList(columns->Pos(), std::move(orderedFields));
            return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
        }

        return IGraphTransformer::TStatus::Ok;
    }

    class TResultCallableExecutionTransformer : public TGraphTransformerBase {
    public:
        TResultCallableExecutionTransformer(const TIntrusivePtr<TResultProviderConfig>& config)
            : Config(config)
        {
            YQL_ENSURE(!Config->Types.AvailablePureResultDataSources.empty());
        }

        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;

            TString uniqId = TStringBuilder() << '#' << input->UniqueId();
            YQL_LOG_CTX_SCOPE(uniqId);

            YQL_ENSURE(input->Type() == TExprNode::Callable);
            TExprBase node(input);
            if (node.Maybe<TResFill>() || node.Maybe<TResPull>() || node.Maybe<TResIf>() || node.Maybe<TResFor>()) {
                auto provider = Config->Types.DataSourceMap.FindPtr(input->Child(5)->Content());
                Y_ENSURE(provider, "DataSource not exist: " << input->Child(5)->Content());
                if (node.Maybe<TResPull>()) {
                    return HandleFillOrPull<TPull>(node, output, ctx, *(*provider));
                } else {
                    return HandleFillOrPull<TResult>(node, output, ctx, *(*provider));
                }
            }

            if (input->Content() == CommitName) {
                if (ResultWriter) {
                    TExprBase commitChild(input->ChildPtr(0));

                    bool overflow = commitChild.Maybe<TResPull>() ? PullOverflow : FillOverflow;
                    ui64& committedSize = commitChild.Maybe<TResPull>() ? CommittedPullSize : CommittedFillSize;

                    if (!ResultWriter->IsDiscard()) {
                        ResultWriter->Commit(overflow);
                        Config->CommittedResults.push_back(TString(ResultWriter->Str()));
                        committedSize += Config->CommittedResults.back().size();
                    }

                    ResultWriter.Reset();
                }

                input->SetState(TExprNode::EState::ExecutionComplete);
                input->SetResult(ctx.NewWorld (input->Pos()));
                return TStatus::Ok;
            }

            if (input->Content() == ConfigureName) {
                input->SetState(TExprNode::EState::ExecutionComplete);
                input->SetResult(ctx.NewWorld(input->Pos()));
                return TStatus::Ok;
            }

            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Failed to execute node: " << input->Content()));
            return TStatus::Ok;
        }

        NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
            Y_UNUSED(input);
            YQL_ENSURE(DelegatedProvider);
            YQL_ENSURE(DelegatedNode);
            YQL_ENSURE(DelegatedNodeOutput);
            return DelegatedProvider->GetCallableExecutionTransformer()
                .GetAsyncFuture(*DelegatedNode);
        }

        TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            output = input;
            YQL_ENSURE(DelegatedProvider);
            YQL_ENSURE(DelegatedNode);
            YQL_ENSURE(DelegatedNodeOutput);
            auto status = DelegatedProvider->GetCallableExecutionTransformer()
                .ApplyAsyncChanges(DelegatedNode, DelegatedNodeOutput, ctx);
            if (status == TStatus::Repeat && input != DelegatedNodeOutput->TailPtr()) {
                output = DelegatedNodeOutput->TailPtr();
            } else {
                FinishNode(*input, ctx, status);
            }
            return status;
        }

        void Rewind() final {
            DelegatedProvider = nullptr;
            DelegatedNode = nullptr;
            DelegatedNodeOutput = nullptr;

            CommittedPullSize = 0;
            PullOverflow = false;

            CommittedFillSize = 0;
            FillOverflow = false;

            ResultWriter.Drop();
        }

   private:
        template <class TTarget>
        bool& GetOverflowFlagAndCommitedSize(ui64& committed);

        template <class TTarget>
        TStatus HandleFillOrPull(TExprBase input, TExprNode::TPtr& output, TExprContext& ctx, IDataProvider& provider) {
            auto requireWorld = RequireChild(input.Ref(), TResBase::idx_World);
            auto requireData = input.Maybe<TResPull>() ? RequireChild(input.Ref(), TResPull::idx_Data) : IGraphTransformer::TStatus::Ok;
            auto requireStatus = requireWorld.Combine(requireData);
            if (requireStatus.Level != IGraphTransformer::TStatus::Ok) {
                return requireStatus;
            }

            bool needWriter = true;
            TMaybeNode<TExprBase> dataNode;
            TMaybeNode<TCoNameValueTupleList> options;
            if (input.Maybe<TResIf>()) {
                if (input.Ref().HasResult()) {
                    auto resultYsonString = input.Ref().GetResult().Content();
                    auto resultNode = NYT::NodeFromYsonString(TString(resultYsonString), ::NYson::EYsonType::Node);
                    YQL_ENSURE(resultNode.IsMap());
                    auto resultBoolNode = resultNode.AsMap()["Data"];
                    YQL_ENSURE(resultBoolNode.IsBool());
                    const bool predicate = resultBoolNode.AsBool();

                    auto branchStatus = RequireChild(input.Ref(), predicate ? TResIf::idx_Then : TResIf::idx_Else);
                    if (branchStatus.Level != IGraphTransformer::TStatus::Ok) {
                        return branchStatus;
                    }

                    input.Ptr()->SetResult(ctx.NewWorld(input.Pos()));
                    input.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
                    return TStatus::Ok;
                }

                needWriter = false;
                dataNode = input.Cast<TResIf>().Condition();
                options = input.Cast<TResIf>().Settings();
            } else if (input.Maybe<TResFor>()) {
                const auto& forNode = input.Cast<TResFor>();
                if (forNode.Total().Value()) {
                    // has total, run active node if any
                    const auto& items = forNode.Items();
                    auto total = FromString<ui32>(forNode.Total().Value());
                    auto current = FromString<ui32>(forNode.Current().Value());
                    if ((current > total) || (total && current >= total)) {
                        auto zero = ctx.NewAtom(TPositionHandle(), "0", TNodeFlags::Default);
                        zero->SetTypeAnn(ctx.MakeType<TUnitExprType>());
                        zero->SetState(TExprNode::EState::ConstrComplete);
                        zero->SetDependencyScope(nullptr, nullptr); // HOTFIX for CSEE
                        input.Ptr()->ChildRef(TResFor::idx_Current) = std::move(zero); //FIXME: Don't use ChilfRef
                        input.Ptr()->SetResult(ctx.NewWorld(input.Pos()));
                        input.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
                        return TStatus::Ok;
                    }

                    const auto& active = forNode.Active();
                    if (active.Ref().IsCallable("Void")) {
                        // select new active node
                        TExprNode::TPtr active;
                        if (total == 0) {
                            // use else block
                            const auto& elseLambda = forNode.Else();
                            active = ctx.ReplaceNode(elseLambda.Body().Ptr(), elseLambda.Args().Arg(0).Ref(), forNode.World().Ptr());
                        } else {
                            // use some list item
                            auto listNode = items.Raw();
                            if (listNode->IsCallable("Just")) {
                                listNode = listNode->Child(0);
                            }

                            ui32 index = current;
                            if (listNode->IsCallable("List")) {
                                ++index;
                            }

                            auto listElem = listNode->Child(index);
                            const auto& iterLambda = forNode.Iter();
                            active = ctx.ReplaceNodes(iterLambda.Body().Ptr(), { {
                                iterLambda.Args().Arg(0).Raw(), forNode.World().Ptr()
                                }, { iterLambda.Args().Arg(1).Raw(), listElem } });
                        }

                        output = Build<TResFor>(ctx, forNode.Pos())
                            .World(forNode.World())
                            .DataSink(forNode.DataSink())
                            .Items(forNode.Items())
                            .Iter(forNode.Iter())
                            .Else(forNode.Else())
                            .DelegatedSource(forNode.DelegatedSource())
                            .Settings(forNode.Settings())
                            .Total(forNode.Total())
                            .Current()
                                .Value(ToString(current))
                            .Build()
                            .Active(active)
                            .Done().Ptr();

                        return IGraphTransformer::TStatus(TStatus::Repeat, true);
                    } else {
                        auto status = RequireChild(input.Ref(), TResFor::idx_Active);
                        if (status.Level != IGraphTransformer::TStatus::Ok) {
                            return status;
                        }

                        ++current;
                        // active node complete, drop it
                        output = Build<TResFor>(ctx, forNode.Pos())
                            .World(forNode.World())
                            .DataSink(forNode.DataSink())
                            .Items(forNode.Items())
                            .Iter(forNode.Iter())
                            .Else(forNode.Else())
                            .DelegatedSource(forNode.DelegatedSource())
                            .Settings(forNode.Settings())
                            .Total(forNode.Total())
                            .Current()
                                .Value(ToString(current))
                            .Build()
                            .template Active<TCoVoid>()
                            .Build()
                            .Done().Ptr();

                        return IGraphTransformer::TStatus(TStatus::Repeat, true);
                    }
                } else if (input.Ref().HasResult()) {
                    // parse list
                    auto resultYsonString = input.Ref().GetResult().Content();
                    auto resultNode = NYT::NodeFromYsonString(TString(resultYsonString), ::NYson::EYsonType::Node);
                    YQL_ENSURE(resultNode.IsMap());
                    auto resultDataNode = resultNode.AsMap()["Data"];

                    const auto& itemsNode = forNode.Items().Ref();

                    TScopedAlloc alloc(__LOCATION__);
                    TTypeEnvironment env(alloc);
                    TStringStream err;
                    NKikimr::NMiniKQL::TTypeBuilder typeBuilder(env);
                    TType* mkqlType = NCommon::BuildType(*itemsNode.GetTypeAnn(), typeBuilder, err);
                    if (!mkqlType) {
                        ctx.AddError(TIssue(ctx.GetPosition(itemsNode.Pos()), TStringBuilder() << "Failed to process type: " << err.Str()));
                        return TStatus::Error;
                    }

                    TMemoryUsageInfo memInfo("Eval");
                    THolderFactory holderFactory(alloc.Ref(), memInfo);
                    auto value = NCommon::ParseYsonNodeInResultFormat(holderFactory, resultDataNode, mkqlType, &err);
                    if (!value) {
                        ctx.AddError(TIssue(ctx.GetPosition(itemsNode.Pos()), TStringBuilder() << "Failed to parse data: " << err.Str()));
                        return TStatus::Error;
                    }

                    // build expr literal & total/active
                    auto itemsLiteral = NCommon::ValueToExprLiteral(itemsNode.GetTypeAnn(), *value, ctx, itemsNode.Pos());
                    ui32 totalItems = 0;
                    auto listNode = itemsLiteral;
                    if (listNode->IsCallable("Just")) {
                        listNode = listNode->Child(0);
                    }

                    if (listNode->IsCallable("AsList")) {
                        totalItems = listNode->ChildrenSize();
                    } else if (listNode->IsCallable("List")) {
                        totalItems = listNode->ChildrenSize() - 1;
                    }

                    output = Build<TResFor>(ctx, forNode.Pos())
                        .World(forNode.World())
                        .DataSink(forNode.DataSink())
                        .Items(itemsLiteral)
                        .Iter(forNode.Iter())
                        .Else(forNode.Else())
                        .DelegatedSource(forNode.DelegatedSource())
                        .Settings(forNode.Settings())
                        .Total()
                            .Value(ToString(totalItems))
                        .Build()
                        .Current()
                            .Value("0")
                        .Build()
                        .Active<TCoVoid>()
                        .Build()
                        .Done().Ptr();

                    return IGraphTransformer::TStatus(TStatus::Repeat, true);
                }

                needWriter = false;
                dataNode = forNode.Items();
                options = forNode.Settings();
            } else {
                dataNode = input.Cast<TResWriteBase>().Data();
                options = input.Cast<TResWriteBase>().Settings();
            }

            DelegatedProvider = &provider;
            auto fillSettings = Config->FillSettings;
            auto resultSize = ResultWriter ? ResultWriter->Size() : 0;

            ui64 committedSize;
            bool& overflow = GetOverflowFlagAndCommitedSize<TTarget>(committedSize);

            if (fillSettings.AllResultsBytesLimit && committedSize + resultSize >= *fillSettings.AllResultsBytesLimit) {
                overflow = true;
            }

            if (fillSettings.AllResultsBytesLimit) {
                if (!overflow && committedSize <= *fillSettings.AllResultsBytesLimit) {
                    *fillSettings.AllResultsBytesLimit -= committedSize;
                } else {
                    *fillSettings.AllResultsBytesLimit = 0;
                }
            }

            auto atomType = ctx.MakeType<TUnitExprType>();
            auto rowsLimit = fillSettings.RowsLimitPerWrite;
            bool discard = false;
            TString label;
            bool unordered = false;
            for (auto setting : options.Cast()) {
                if (setting.Name().Value() == "take") {
                    auto value = FromString<ui64>(setting.Value().Cast<TCoAtom>().Value());
                    if (rowsLimit) {
                        rowsLimit = Min(*rowsLimit, value);
                    } else {
                        rowsLimit = value;
                    }
                } else if (setting.Name().Value() == "discard") {
                    discard = true;
                } else if (setting.Name().Value() == "label") {
                    label = TString(setting.Value().Cast<TCoAtom>().Value());
                } else if (setting.Name().Value() == "unordered") {
                    unordered = true;
                }
            }

            TString publicId;
            if (auto id = Config->Types.TranslateOperationId(input.Ref().UniqueId())) {
                publicId = ToString(*id);
            }

            if (needWriter && !ResultWriter) {
                YQL_ENSURE(Config->WriterFactory);
                ResultWriter = Config->WriterFactory();
                ResultWriter->Init(discard, label, Config->SupportsResultPosition ?
                    TMaybe<TPosition>(ctx.GetPosition(input.Pos())) : Nothing(), unordered);
            }

            if (input.Maybe<TResIf>() || input.Maybe<TResFor>()) {
                fillSettings = IDataProvider::TFillSettings();
                fillSettings.AllResultsBytesLimit.Clear();
                discard = fillSettings.Discard = false;
                fillSettings.Format = IDataProvider::EResultFormat::Yson;
                fillSettings.FormatDetails = ToString((ui32)NYson::EYsonFormat::Binary);
                fillSettings.RowsLimitPerWrite.Clear();
                rowsLimit.Clear();
            }

            DelegatedNode = Build<TTarget>(ctx, input.Pos())
                .Input(dataNode.Cast())
                .BytesLimit()
                    .Value(fillSettings.AllResultsBytesLimit ? ToString(*fillSettings.AllResultsBytesLimit) : TString())
                .Build()
                .RowsLimit()
                    .Value(rowsLimit ? ToString(*rowsLimit) : TString())
                .Build()
                .FormatDetails()
                    .Value(fillSettings.FormatDetails)
                .Build()
                .Settings(options.Cast())
                .Format()
                    .Value(ToString((ui32)fillSettings.Format))
                .Build()
                .PublicId()
                    .Value(publicId)
                .Build()
                .Discard()
                    .Value(ToString(discard))
                .Build()
                .Origin(input)
                .Done().Ptr();

            for (auto idx: {TResOrPullBase::idx_BytesLimit, TResOrPullBase::idx_RowsLimit, TResOrPullBase::idx_FormatDetails,
                TResOrPullBase::idx_Format, TResOrPullBase::idx_PublicId, TResOrPullBase::idx_Discard }) {
                DelegatedNode->Child(idx)->SetTypeAnn(atomType);
                DelegatedNode->Child(idx)->SetState(TExprNode::EState::ConstrComplete);
            }

            DelegatedNode->SetTypeAnn(input.Ref().GetTypeAnn());
            DelegatedNode->SetState(TExprNode::EState::ConstrComplete);
            input.Ptr()->SetState(TExprNode::EState::ExecutionInProgress);
            auto status = DelegatedProvider->GetCallableExecutionTransformer().Transform(DelegatedNode, DelegatedNodeOutput, ctx);
            if (status.Level != TStatus::Async) {
                FinishNode(*input.Ptr(), ctx, status);
            }

            return status;
        }

        void FinishNode(TExprNode& input, TExprContext& ctx, IGraphTransformer::TStatus status) {
            if (status.Level == TStatus::Ok) {
                auto data = DelegatedNode->GetResult().Content();
                const bool needWriter = input.Content() != TResIf::CallableName()
                    && input.Content() != TResFor::CallableName();
                if (needWriter) {
                    ResultWriter->Write(data);

                    input.SetResult(ctx.NewAtom(input.Pos(), ""));
                    input.SetState(TExprNode::EState::ExecutionComplete);
                } else {
                    input.SetResult(ctx.NewAtom(input.Pos(), data));
                    input.SetState(TExprNode::EState::ExecutionRequired);
                }
            } else if (status.Level == TStatus::Error) {
                if (const auto issies = ctx.AssociativeIssues.extract(DelegatedNode.Get())) {
                    ctx.IssueManager.RaiseIssues(issies.mapped());
                }
            } else {
                input.SetState(TExprNode::EState::ExecutionRequired);
            }

            DelegatedProvider = nullptr;
            DelegatedNode = nullptr;
            DelegatedNodeOutput = nullptr;
        }

    private:
        const TIntrusivePtr<TResultProviderConfig> Config;
        IDataProvider* DelegatedProvider = nullptr;
        TExprNode::TPtr DelegatedNode;
        TExprNode::TPtr DelegatedNodeOutput;

        ui64 CommittedPullSize = 0;
        bool PullOverflow = false;

        ui64 CommittedFillSize = 0;
        bool FillOverflow = false;

        TIntrusivePtr<IResultWriter> ResultWriter;
    };

    template <class TTarget>
    bool& TResultCallableExecutionTransformer::GetOverflowFlagAndCommitedSize(ui64& committed) {
        committed = CommittedFillSize;
        return FillOverflow;
    }

    template<>
    bool& TResultCallableExecutionTransformer::GetOverflowFlagAndCommitedSize<TPull>(ui64& committed) {
        committed = CommittedPullSize;
        return PullOverflow;
    }

    class TResultTrackableNodeProcessor : public TTrackableNodeProcessorBase {
    public:
        TResultTrackableNodeProcessor(const TIntrusivePtr<TResultProviderConfig>& config)
            : Config(config)
        {}

        void GetUsedNodes(const TExprNode& input, TVector<TString>& usedNodeIds) override {
            usedNodeIds.clear();
            if (TMaybeNode<TResFill>(&input) || TMaybeNode<TResPull>(&input) || TMaybeNode<TResIf>(&input)
                || TMaybeNode<TResFor>(&input)) {
                auto provider = Config->Types.DataSourceMap.FindPtr(input.Child(5)->Content());
                Y_ENSURE(provider, "DataSource not exist: " << input.Child(5)->Content());
                (*provider)->GetTrackableNodeProcessor().GetUsedNodes(input, usedNodeIds);
            }
        }
    private:
        const TIntrusivePtr<TResultProviderConfig> Config;
    };

    class TPhysicalFinalizingTransformer final : public TSyncTransformerBase {
    public:
        TPhysicalFinalizingTransformer(const TIntrusivePtr<TResultProviderConfig>& config)
            : Config(config) {}

        TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
            TOptimizeExprSettings settings(&Config->Types);
            settings.ProcessedNodes = &PhysicalOptProcessedNodes;
            TStatus status = OptimizeExprEx(input, output,
                [&](const TExprNode::TPtr& node, TExprContext& ctx, IOptimizationContext& optCtx) -> TExprNode::TPtr {
                auto ret = node;
                if (auto maybeWrite = TMaybeNode<TResWrite>(node)) {
                    auto resWrite = maybeWrite.Cast();
                    bool isRef = false;
                    bool isAutoRef = false;
                    for (auto child: resWrite.Settings()) {
                        if (child.Name().Value() == "ref") {
                            isRef = true;
                        }

                        if (child.Name().Value() == "autoref") {
                            isAutoRef = true;
                        }
                    }

                    auto writeInput = resWrite.Data();
                    for (auto& source : Config->Types.DataSources) {
                        TSyncMap syncList;
                        bool canRef;
                        if (source->CanPullResult(writeInput.Ref(), syncList, canRef)) {
                            auto newInput = writeInput;

                            if (isRef && !canRef) {
                                ctx.AddError(TIssue(ctx.GetPosition(writeInput.Pos()), TStringBuilder() <<
                                    "RefSelect mode isn't supported by provider: " << source->GetName()));
                                return nullptr;
                            }

                            auto fillSettings = Config->FillSettings;
                            if (!isRef && (!isAutoRef || !canRef)) {
                                for (auto setting: resWrite.Settings()) {
                                    if (setting.Name().Value() == "take") {
                                        auto value = FromString<ui64>(setting.Value().Cast<TCoAtom>().Value());
                                        if (fillSettings.RowsLimitPerWrite) {
                                            fillSettings.RowsLimitPerWrite = Min(*fillSettings.RowsLimitPerWrite, value);
                                        } else {
                                            fillSettings.RowsLimitPerWrite = value;
                                        }
                                    }
                                }

                                if (fillSettings.RowsLimitPerWrite) {
                                    *fillSettings.RowsLimitPerWrite += 1;
                                }
                            } else {
                                fillSettings.RowsLimitPerWrite.Clear();
                            }
                            newInput = TExprBase(source->OptimizePull(newInput.Ptr(), fillSettings, ctx, optCtx));

                            ret = Build<TResPull>(ctx, resWrite.Pos())
                                .World(ApplySyncListToWorld(resWrite.World().Ptr(), syncList, ctx))
                                .DataSink(resWrite.DataSink())
                                .Key(resWrite.Key())
                                .Data(newInput)
                                .Settings(resWrite.Settings())
                                .DelegatedSource()
                                    .Value(source->GetName())
                                .Build()
                                .Done().Ptr();

                            YQL_CLOG(INFO, ProviderResult) << "ResPull";
                            return ret;
                        }
                    }

                    if (!isRef) {
                        auto data = resWrite.Data();
                        if (auto unordered = data.Maybe<TCoUnorderedBase>()) {
                            data = unordered.Cast().Input();
                        }

                        TSyncMap syncList;
                        if (IsPureIsolatedLambda(writeInput.Ref(), &syncList)) {
                            auto cleanup = DefaultCleanupWorld(data.Ptr(), ctx);
                            if (!cleanup) {
                                return nullptr;
                            }

                            ret = Build<TResFill>(ctx, resWrite.Pos())
                                .World(ApplySyncListToWorld(resWrite.World().Ptr(), syncList, ctx))
                                .DataSink(resWrite.DataSink())
                                .Key(resWrite.Key())
                                .Data(cleanup)
                                .Settings(resWrite.Settings())
                                .DelegatedSource()
                                    .Value(Config->Types.GetDefaultDataSource())
                                .Build()
                                .Done().Ptr();

                            YQL_CLOG(INFO, ProviderResult) << "ResFill";
                            return ret;
                        }

                        for (auto& source : Config->Types.DataSources) {
                            TSyncMap syncList;
                            if (source->CanBuildResult(writeInput.Ref(), syncList)) {
                                auto cleanup = source->CleanupWorld(data.Ptr(), ctx);
                                if (!cleanup) {
                                    return nullptr;
                                }

                                ret = Build<TResFill>(ctx, resWrite.Pos())
                                    .World(ApplySyncListToWorld(resWrite.World().Ptr(), syncList, ctx))
                                    .DataSink(resWrite.DataSink())
                                    .Key(resWrite.Key())
                                    .Data(cleanup)
                                    .Settings(resWrite.Settings())
                                    .DelegatedSource()
                                        .Value(source->GetName())
                                    .Build()
                                    .Done().Ptr();

                                YQL_CLOG(INFO, ProviderResult) << "ResFill";
                                return ret;
                            }
                        }
                    }
                } else if (node->Content() == IfName) {
                    TSyncMap syncList;
                    auto foundDataSource = FindDataSource(*node->Child(1), syncList);
                    if (!foundDataSource.empty()) {
                        auto provider = Config->Types.DataSourceMap.FindPtr(foundDataSource);
                        Y_ENSURE(provider, "DataSource doesn't exist: " << foundDataSource);
                        auto cleanup = (*provider)->CleanupWorld(node->ChildPtr(1), ctx);
                        if (!cleanup) {
                            return nullptr;
                        }

                        ret = Build<TResIf>(ctx, node->Pos())
                            .World(ApplySyncListToWorld(node->ChildPtr(0), syncList, ctx))
                            .DataSink()
                            .Build()
                            .Condition(cleanup)
                            .Then(node->ChildPtr(2))
                            .Else(node->ChildPtr(3))
                            .DelegatedSource()
                                .Value(foundDataSource)
                            .Build()
                            .Settings()
                            .Build()
                            .Done().Ptr();

                        YQL_CLOG(INFO, ProviderResult) << "ResIf";
                        return ret;
                    }
                } else if (node->Content() == ForName) {
                    TSyncMap syncList;
                    auto foundDataSource = FindDataSource(*node->Child(1), syncList);
                    if (!foundDataSource.empty()) {
                        auto provider = Config->Types.DataSourceMap.FindPtr(foundDataSource);
                        Y_ENSURE(provider, "DataSource doesn't exist: " << foundDataSource);
                        auto cleanup = (*provider)->CleanupWorld(node->ChildPtr(1), ctx);
                        if (!cleanup) {
                            return nullptr;
                        }

                        ret = Build<TResFor>(ctx, node->Pos())
                            .World(ApplySyncListToWorld(node->ChildPtr(0), syncList, ctx))
                            .DataSink()
                            .Build()
                            .Items(cleanup)
                            .Iter(node->ChildPtr(2))
                            .Else(node->ChildPtr(3))
                            .DelegatedSource()
                                .Value(foundDataSource)
                            .Build()
                            .Settings()
                            .Build()
                            .Total()
                                .Value("")
                            .Build()
                            .Current()
                                .Value("")
                            .Build()
                            .Active<TCoVoid>()
                            .Build()
                            .Done().Ptr();

                        YQL_CLOG(INFO, ProviderResult) << "ResFor";
                        return ret;
                    }
                }

                return ret;
            }, ctx, settings);

            return status;
        }

        void Rewind() final {
            PhysicalOptProcessedNodes.clear();
        }

    private:
        TString FindDataSource(const TExprNode& node, TSyncMap& syncList) const {
            syncList.clear();
            TString foundDataSource;
            if (IsPureIsolatedLambda(node)) {
                foundDataSource = Config->Types.GetDefaultDataSource();
            }

            if (foundDataSource.empty()) {
                for (auto& source : Config->Types.DataSources) {
                    syncList.clear();
                    if (source->CanBuildResult(node, syncList)) {
                        foundDataSource = TString(source->GetName());
                        break;
                    }
                }
            }

            return foundDataSource;
        }

    private:
        const TIntrusivePtr<TResultProviderConfig> Config;
        TProcessedNodesSet PhysicalOptProcessedNodes;
    };

    class TResultProvider : public TDataProviderBase {
    public:
        struct TFunctions {
            THashSet<TStringBuf> Names;

            TFunctions() {
                Names.insert(TResWrite::CallableName());
                Names.insert(TResFill::CallableName());
                Names.insert(TResPull::CallableName());
                Names.insert(TResIf::CallableName());
                Names.insert(TResFor::CallableName());
            }
        };

        TResultProvider(const TIntrusivePtr<TResultProviderConfig>& config)
            : Config(config)
            , TrackableNodeProcessor(config)
        {}

        TStringBuf GetName() const override {
            return ResultProviderName;
        }

        bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
            if (!EnsureArgsCount(node, 1, ctx)) {
                return false;
            }

            cluster = Nothing();
            return true;
        }

        bool CanParse(const TExprNode& node) override {
            return ResultProviderFunctions().contains(node.Content()) || node.Content() == ConfigureName;
        }

        void FillModifyCallables(THashSet<TStringBuf>& callables) override {
            callables.insert(TResWrite::CallableName());
        }

        IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
            Y_UNUSED(instantOnly);
            if (!TypeAnnotationTransformer) {
                TypeAnnotationTransformer = CreateFunctorTransformer(
                    [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx)->IGraphTransformer::TStatus {
                    output = input;

                    if (auto maybeRes = TMaybeNode<TResWriteBase>(input)) {
                        auto res = maybeRes.Cast();
                        if (!EnsureArgsCount(*input, res.Maybe<TResWrite>() ? 5 : 6, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*res.Ref().Child(TResWriteBase::idx_World), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataSink(*res.Ref().Child(TResWriteBase::idx_DataSink), ResultProviderName, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!res.Ref().Child(TResWriteBase::idx_Key)->IsCallable("Key") || res.Ref().Child(TResWriteBase::idx_Key)->ChildrenSize() > 0) {
                            ctx.AddError(TIssue(ctx.GetPosition(res.Ref().Child(TResWriteBase::idx_Key)->Pos()), "Expected empty key"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsurePersistable(res.Data().Ref(), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto settings = res.Ref().Child(TResWriteBase::idx_Settings);
                        if (!EnsureTuple(*settings, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        TExprNode::TPtr columns;
                        bool hasRef = false;
                        bool hasAutoRef = false;
                        ui32 settingPos = 0;
                        for (auto& setting : settings->Children()) {
                            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!EnsureAtom(*setting->Child(0), ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            auto content = setting->Child(0)->Content();
                            if (content == "ref") {
                                hasRef = true;
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "autoref") {
                                hasAutoRef = true;
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "type") {
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "take") {
                                if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                if (!EnsureAtom(*setting->Child(1), ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                ui64 limit = 0;
                                if (!TryFromString(setting->Child(1)->Content(), limit)) {
                                    ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), "Expected unsigned integer"));
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "columns") {
                                if (columns) {
                                    ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), "columns is already used"));
                                    return IGraphTransformer::TStatus::Error;
                                }

                                if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                columns = setting->ChildPtr(1);
                                auto status = ValidateColumns(columns, res.Data().Ref().GetTypeAnn(), ctx);
                                if (status.Level != IGraphTransformer::TStatus::Ok) {
                                    if (status.Level == IGraphTransformer::TStatus::Repeat) {
                                        if (!columns) {
                                            output = ctx.ChangeChild(*input, 4, RemoveSetting(*input->Child(4), "columns", ctx));
                                        } else {
                                            auto newSetting = ctx.ChangeChild(*setting, 1, std::move(columns));
                                            auto newSettings = ctx.ChangeChild(*settings, settingPos, std::move(newSetting));
                                            output = ctx.ChangeChild(*input, 4, std::move(newSettings));
                                        }
                                    }

                                    return status;
                                }
                            } else if (content == "freezeColumns") {
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "discard") {
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "label") {
                                if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }

                                if (!EnsureAtom(*setting->Child(1), ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else if (content == "unordered") {
                                if (!EnsureTupleMaxSize(*setting, 1, ctx)) {
                                    return IGraphTransformer::TStatus::Error;
                                }
                            } else {
                                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), "Expected label,discard,ref,autoref,type,unordered,take or columns atom"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (hasRef && hasAutoRef) {
                                ctx.AddError(TIssue(ctx.GetPosition(setting->Pos()), "Either ref or autoref may be specified, not both"));
                                return IGraphTransformer::TStatus::Error;
                            }

                            ++settingPos;
                        }

                        if (auto right = res.Data().Maybe<TCoRight>()) {
                            auto source = right.Cast().Input();
                            if (!source.Maybe<TCoCons>()) {
                                const TIntrusivePtr<IDataProvider>* provider = nullptr;

                                if (source.Ref().Type() == TExprNode::Callable || source.Ref().ChildrenSize() >= 2) {
                                    if (source.Ref().Child(1)->IsCallable("DataSource")) {
                                        auto name = source.Ref().Child(1)->Child(0)->Content();
                                        provider = Config->Types.DataSourceMap.FindPtr(name);
                                        Y_ENSURE(provider, "DataSource doesn't exist: " << name);
                                    }

                                    if (source.Ref().Child(1)->IsCallable("DataSink")) {
                                        auto name = source.Ref().Child(1)->Child(0)->Content();
                                        provider = Config->Types.DataSinkMap.FindPtr(name);
                                        Y_ENSURE(provider, "DataSink doesn't exist: " << name);
                                    }
                                }

                                if (!provider) {
                                    ctx.AddError(TIssue(ctx.GetPosition(res.Data().Pos()), "Expected Right! over Datasource or Datasink"));
                                    return IGraphTransformer::TStatus::Error;
                                }
                            }
                        }

                        if (res.Maybe<TResTransientBase>()) {
                            auto resTransient = res.Cast<TResTransientBase>();
                            if (!EnsureAtom(*resTransient.Ref().Child(TResTransientBase::idx_DelegatedSource), ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!Config->Types.DataSourceMap.FindPtr(resTransient.DelegatedSource().Value())) {
                                ctx.AddError(TIssue(ctx.GetPosition(resTransient.DelegatedSource().Pos()),
                                    TStringBuilder() << "DataSource is not found: " << resTransient.DelegatedSource().Value()));
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        if (res.Data().Ref().IsCallable("AssumeColumnOrder")) {
                            if (!HasSetting(res.Settings().Ref(), "freezeColumns")) {
                                auto dataOrder = Config->Types.LookupColumnOrder(res.Data().Ref());
                                YQL_ENSURE(dataOrder);

                                YQL_CLOG(INFO, ProviderResult) << "Setting result column order: " << FormatColumnOrder(dataOrder);
                                auto settings = RemoveSetting(res.Settings().Ref(), "columns", ctx);
                                TExprNodeList columnsList;
                                for (auto& col : *dataOrder) {
                                    columnsList.push_back(ctx.NewAtom(settings->Pos(), col));
                                }
                                settings = AddSetting(*settings, settings->Pos(), "columns", ctx.NewList(settings->Pos(), std::move(columnsList)), ctx);
                                settings = AddSetting(*settings, settings->Pos(), "freezeColumns", nullptr, ctx);
                                output = ctx.ChangeChild(*input, TResWriteBase::idx_Settings, std::move(settings));
                            }
                            output = ctx.ChangeChild(*output, TResWriteBase::idx_Data, res.Data().Ref().HeadPtr());
                            return IGraphTransformer::TStatus::Repeat;
                        }

                        input->SetTypeAnn(res.World().Ref().GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }
                    else if (auto maybeIf = TMaybeNode<TResIf>(input)) {
                        if (!EnsureArgsCount(*input, 7, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*input->Child(TResIf::idx_World), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataSink(*input->Child(TResIf::idx_DataSink), ResultProviderName, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataType(*input->Child(TResIf::idx_Condition), EDataSlot::Bool, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*input->Child(TResIf::idx_Then), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*input->Child(TResIf::idx_Else), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*input->Child(TResIf::idx_DelegatedSource), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!Config->Types.DataSourceMap.FindPtr(input->Child(TResIf::idx_DelegatedSource)->Content())) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TResIf::idx_DelegatedSource)->Pos()),
                                TStringBuilder() << "DataSource is not found: " << input->Child(TResIf::idx_DelegatedSource)->Content()));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto settings = input->Child(TResIf::idx_Settings);
                        if (!EnsureTupleSize(*settings, 0, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        input->SetTypeAnn(input->Child(TResIf::idx_World)->GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }
                    else if (auto maybeFor = TMaybeNode<TResFor>(input)) {
                        if (!EnsureArgsCount(*input, 10, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*input->Child(TResFor::idx_World), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataSink(*input->Child(TResFor::idx_DataSink), ResultProviderName, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsurePersistable(*input->Child(TResFor::idx_Items), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto listType = RemoveOptionalType(input->Child(TResFor::idx_Items)->GetTypeAnn());
                        if (!EnsureListType(input->Child(TResFor::idx_Items)->Pos(), *listType, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto itemType = listType->Cast<TListExprType>()->GetItemType();
                        auto status = ConvertToLambda(input->ChildRef(TResFor::idx_Iter), ctx, 2);
                        if (status.Level != IGraphTransformer::TStatus::Ok) {
                            return status;
                        }

                        auto& lambda1 = input->ChildRef(TResFor::idx_Iter);
                        if (!UpdateLambdaAllArgumentsTypes(lambda1, { ctx.MakeType<TWorldExprType>(), itemType }, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!lambda1->GetTypeAnn()) {
                            return IGraphTransformer::TStatus::Repeat;
                        }

                        if (!EnsureWorldType(*lambda1->Child(1), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        status = ConvertToLambda(input->ChildRef(TResFor::idx_Else), ctx, 1);
                        if (status.Level != IGraphTransformer::TStatus::Ok) {
                            return status;
                        }

                        auto& lambda2 = input->ChildRef(TResFor::idx_Else);
                        if (!UpdateLambdaAllArgumentsTypes(lambda2, { ctx.MakeType<TWorldExprType>() }, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!lambda2->GetTypeAnn()) {
                            return IGraphTransformer::TStatus::Repeat;
                        }

                        if (!EnsureWorldType(*lambda2->Child(1), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*input->Child(TResFor::idx_DelegatedSource), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!Config->Types.DataSourceMap.FindPtr(input->Child(TResFor::idx_DelegatedSource)->Content())) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TResFor::idx_DelegatedSource)->Pos()),
                                TStringBuilder() << "DataSource is not found: " << input->Child(TResFor::idx_DelegatedSource)->Content()));
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto settings = input->Child(TResFor::idx_Settings);
                        if (!EnsureTupleSize(*settings, 0, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*input->Child(TResFor::idx_Total), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto total = input->Child(TResFor::idx_Total)->Content();
                        ui32 totalValue = 0;
                        if (total && !TryFromString(total, totalValue)) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TResFor::idx_Total)->Pos()),
                                TStringBuilder() << "Expected number, but got: " << total));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*input->Child(TResFor::idx_Current), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto current = input->Child(TResFor::idx_Current)->Content();
                        ui32 currentValue = 0;
                        if (current && !TryFromString(current, currentValue)) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TResFor::idx_Current)->Pos()),
                                TStringBuilder() << "Expected number, but got: " << current));
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!total != !current) {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TResFor::idx_Current)->Pos()),
                                TStringBuilder() << "Current value should be set simultaneously with total value"));
                            return IGraphTransformer::TStatus::Error;
                        }

                        const auto& active = *input->Child(TResFor::idx_Active);
                        if (!active.IsCallable("Void")) {
                            if (!EnsureWorldType(active, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }
                        }

                        input->SetTypeAnn(input->Child(TResIf::idx_World)->GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }
                    else if (auto maybeCommit = TMaybeNode<TCoCommit>(input)) {
                        auto commit = maybeCommit.Cast();
                        auto settings = NCommon::ParseCommitSettings(commit, ctx);

                        if (!settings.EnsureModeEmpty(ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                        if (!settings.EnsureEpochEmpty(ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }
                        if (!settings.EnsureOtherEmpty(ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        input->SetTypeAnn(commit.World().Ref().GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }
                    else if (input->Content() == ConfigureName) {
                        if (!EnsureMinArgsCount(*input, 3, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureWorldType(*input->Child(0), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureSpecificDataSink(*input->Child(1), ResultProviderName, ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        if (!EnsureAtom(*input->Child(2), ctx)) {
                            return IGraphTransformer::TStatus::Error;
                        }

                        auto command = input->Child(2)->Content();
                        if (command == "SizeLimit") {
                            if (!EnsureArgsCount(*input, 4, ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (!EnsureAtom(*input->Child(3), ctx)) {
                                return IGraphTransformer::TStatus::Error;
                            }

                            const auto limitStr = input->Child(3)->Content();
                            ui64 limit;
                            if (!TryFromString(limitStr, limit)) {
                                ctx.AddError(TIssue(ctx.GetPosition(input->Child(3)->Pos()), TStringBuilder() << "expected integer, but got: " << limitStr));
                                return IGraphTransformer::TStatus::Error;
                            }

                            if (Config->FillSettings.AllResultsBytesLimit) {
                                Config->FillSettings.AllResultsBytesLimit = Min(*Config->FillSettings.AllResultsBytesLimit, limit);
                            } else {
                                Config->FillSettings.AllResultsBytesLimit = limit;
                            }

                        } else {
                            ctx.AddError(TIssue(ctx.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Unsupported command: " << command));
                            return IGraphTransformer::TStatus::Error;
                        }

                        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
                        return IGraphTransformer::TStatus::Ok;
                    }

                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "(Result) Unsupported function: " << input->Content()));
                    return IGraphTransformer::TStatus::Error;
                });
            }

            return *TypeAnnotationTransformer;
        }

        TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
            auto ret = node;
            if (node->Content() == WriteName) {
                ret = ctx.RenameNode(*ret, TResWrite::CallableName());
                ret = ctx.ChangeChild(*ret, TResWrite::idx_Data,
                    ctx.Builder(node->Pos())
                        .Callable("RemovePrefixMembers")
                            .Add(0, node->ChildPtr(TResWrite::idx_Data))
                            .List(1)
                                .Atom(0, "_yql_sys_", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Build()
                    );
            }
            else {
                YQL_ENSURE(false, "Expected Write!");
            }

            YQL_CLOG(INFO, ProviderResult) << "RewriteIO";
            return ret;
        }

        IGraphTransformer& GetPhysicalFinalizingTransformer() override {
            if (!PhysicalFinalizingTransformer) {
                PhysicalFinalizingTransformer = new TPhysicalFinalizingTransformer(Config);
            }

            return *PhysicalFinalizingTransformer;
        }

        bool CanExecute(const TExprNode& node) override {
            if (node.Content() == TResFill::CallableName()) {
                return true;
            }

            if (node.Content() == TResPull::CallableName()) {
                return true;
            }

            if (node.Content() == TResIf::CallableName()) {
                return true;
            }

            if (node.Content() == TResFor::CallableName()) {
                return true;
            }

            if (node.Content() == ConfigureName) {
                return true;
            }

            return false;
        }

        bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
            auto getDataProvider = [&]() {
                auto provider = Config->Types.DataSourceMap.FindPtr(node.Child(5)->Content());
                Y_ENSURE(provider, "DataSource doesn't exist: " << node.Child(5)->Content());
                return *provider;
            };

            if (TResTransientBase::Match(&node)) {
                return getDataProvider()->ValidateExecution(TResTransientBase(&node).Data().Ref(), ctx);
            }
            if (TResIf::Match(&node)) {
                return getDataProvider()->ValidateExecution(TResIf(&node).Condition().Ref(), ctx);
            }
            if (TResFor::Match(&node)) {
                return getDataProvider()->ValidateExecution(TResFor(&node).Items().Ref(), ctx);
            }
            return true;
        }

        IGraphTransformer& GetCallableExecutionTransformer() override {
            if (!CallableExecutionTransformer) {
                CallableExecutionTransformer = new TResultCallableExecutionTransformer(Config);
            }

            return *CallableExecutionTransformer;
        }

        void Reset() final {
            TDataProviderBase::Reset();
            if (CallableExecutionTransformer) {
                CallableExecutionTransformer.Reset();
            }
            Config->CommittedResults.clear();
        }

        bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
            if (CanExecute(node)) {
                children.push_back(node.ChildPtr(0));
                if (auto resPull = TMaybeNode<TResPull>(&node)) {
                    children.push_back(resPull.Cast().Data().Ptr());
                } else if (auto resIf = TMaybeNode<TResIf>(&node)) {
                    children.push_back(resIf.Cast().Then().Ptr());
                    children.push_back(resIf.Cast().Else().Ptr());
                } else if (auto resFor = TMaybeNode<TResFor>(&node)) {
                    auto active = resFor.Cast().Active().Ptr();
                    if (!active->IsCallable("Void")) {
                        children.push_back(active);
                    }
                } else if (auto resFill = TMaybeNode<TResFill>(&node)) {
                    const auto provider = Config->Types.DataSourceMap.FindPtr(resFill.Cast().DelegatedSource().Value());
                    Y_ENSURE(provider, "DataSource not exist: " << resFill.Cast().DelegatedSource().Value());
                    (*provider)->GetPlanFormatter().GetResultDependencies(resFill.Cast().Data().Ptr(), children, compact);
                }

                return true;
            }

            return false;
        }

        void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override {
            Y_UNUSED(withLimits);
            if (auto resPull = TMaybeNode<TResPull>(&node)) {
                auto dataSourceName = resPull.Cast().DelegatedSource().Value();
                auto dataSource = Config->Types.DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(dataSource);

                (*dataSource)->GetPlanFormatter().WritePullDetails(resPull.Cast().Data().Ref(), writer);
            }
        }

        TString GetProviderPath(const TExprNode& node) override {
            Y_UNUSED(node);
            return "result";
        }

        TString GetOperationDisplayName(const TExprNode& node) override {
            if (node.Content() == CommitName) {
                return TString::Join(node.Content(), " on result");
            }

            if (auto maybeResFor = TMaybeNode<TResFor>(&node)) {
                auto resFor = maybeResFor.Cast();

                TStringBuilder res;
                res << node.Content();
                if (resFor.Total().Value() && resFor.Total().Value() != "0") {
                    res << ", " << (node.GetState() == TExprNode::EState::ExecutionComplete ?
                        resFor.Total().Value() : resFor.Current().Value()) << "/" << resFor.Total().Value();
                }

                return res;
            }

            return TString(node.Content());
        }

        ITrackableNodeProcessor& GetTrackableNodeProcessor() override {
            return TrackableNodeProcessor;
        }

    private:
        const TIntrusivePtr<TResultProviderConfig> Config;
        TResultTrackableNodeProcessor TrackableNodeProcessor;
        TAutoPtr<IGraphTransformer> TypeAnnotationTransformer;
        TAutoPtr<IGraphTransformer> PhysicalFinalizingTransformer;
        TAutoPtr<IGraphTransformer> CallableExecutionTransformer;
    };
}

TIntrusivePtr<IResultWriter> CreateYsonResultWriter(NYson::EYsonFormat format) {
    return MakeIntrusive<TYsonResultWriter>(format);
}

TIntrusivePtr<IDataProvider> CreateResultProvider(const TIntrusivePtr<TResultProviderConfig>& config) {
    return new TResultProvider(config);
}

const THashSet<TStringBuf>& ResultProviderFunctions() {
    return Singleton<TResultProvider::TFunctions>()->Names;
}

}
