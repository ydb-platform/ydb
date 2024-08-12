#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"
#include "yql_yt_dq_integration.h"

#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/generic/set.h>
#include <util/generic/utility.h>
#include <util/generic/ylimits.h>

#include <algorithm>
#include <iterator>

namespace NYql {

using namespace NNodes;

class TYtDataSinkTrackableNodeProcessor : public TTrackableNodeProcessorBase {
public:
    TYtDataSinkTrackableNodeProcessor(const TYtState::TPtr& state, bool collectNodes)
        : CollectNodes(collectNodes)
        , CleanupTransformer(collectNodes ? CreateYtDataSinkTrackableNodesCleanupTransformer(state) : nullptr)
    {
    }

    void GetUsedNodes(const TExprNode& input, TVector<TString>& usedNodeIds) override {
        usedNodeIds.clear();
        if (!CollectNodes) {
            return;
        }

        if (TMaybeNode<TYtOutputOpBase>(&input)) {
            for (size_t i = TYtOutputOpBase::idx_Output + 1; i < input.ChildrenSize(); ++i) {
                ScanForUsedOutputTables(*input.Child(i), usedNodeIds);
            }
        } else if (TMaybeNode<TYtPublish>(&input)) {
            ScanForUsedOutputTables(*input.Child(TYtPublish::idx_Input), usedNodeIds);
        } else if (TMaybeNode<TYtStatOut>(&input)) {
            ScanForUsedOutputTables(*input.Child(TYtStatOut::idx_Input), usedNodeIds);
        }
    }

    void GetCreatedNodes(const TExprNode& node, TVector<TExprNodeAndId>& created, TExprContext& ctx) override {
        created.clear();
        if (!CollectNodes) {
            return;
        }

        if (auto maybeOp = TMaybeNode<TYtOutputOpBase>(&node)) {
            TString clusterName = TString{maybeOp.Cast().DataSink().Cast<TYtDSink>().Cluster().Value()};
            auto clusterPtr = maybeOp.Cast().DataSink().Ptr();
            for (auto table: maybeOp.Cast().Output()) {
                TString tableName = TString{table.Name().Value()};
                auto tablePtr = table.Ptr();

                TExprNodeAndId nodeAndId;
                nodeAndId.Id = MakeUsedNodeId(clusterName, tableName);
                nodeAndId.Node = ctx.NewList(tablePtr->Pos(), {clusterPtr, tablePtr});

                created.push_back(std::move(nodeAndId));
            }
        }
    }

    IGraphTransformer& GetCleanupTransformer() override {
        return CollectNodes ? *CleanupTransformer : NullTransformer_;
    }

private:
    const bool CollectNodes;
    THolder<IGraphTransformer> CleanupTransformer;
};

class TYtDataSink : public TDataProviderBase {
public:
    TYtDataSink(TYtState::TPtr state)
        : State_(state)
        , IntentDeterminationTransformer_([this]() { return CreateYtIntentDeterminationTransformer(State_); })
        , TypeAnnotationTransformer_([this]() { return CreateYtDataSinkTypeAnnotationTransformer(State_); })
        , ConstraintTransformer_([this]() { return CreateYtDataSinkConstraintTransformer(State_, false); })
        , SubConstraintTransformer_([this]() { return CreateYtDataSinkConstraintTransformer(State_, true); })
        , ExecTransformer_([this]() { return CreateYtDataSinkExecTransformer(State_); })
        , LogicalOptProposalTransformer_([this]() { return CreateYtLogicalOptProposalTransformer(State_); })
        , PhysicalOptProposalTransformer_([this]() { return CreateYtPhysicalOptProposalTransformer(State_); })
        , PhysicalFinalizingTransformer_([this]() {
            auto transformer = CreateYtPhysicalFinalizingTransformer(State_);
            if (State_->IsHybridEnabled())
                transformer = CreateYtDqHybridTransformer(State_, std::move(transformer));
            return transformer;
        })
        , FinalizingTransformer_([this]() { return CreateYtDataSinkFinalizingTransformer(State_); })
        , TrackableNodeProcessor_([this]() {
            auto mode = GetReleaseTempDataMode(*State_->Configuration);
            bool collectNodes = mode == EReleaseTempDataMode::Immediate;
            return MakeHolder<TYtDataSinkTrackableNodeProcessor>(State_, collectNodes);
        })
    {
    }

    TStringBuf GetName() const override {
        return YtProviderName;
    }

    IGraphTransformer& GetIntentDeterminationTransformer() override {
        return *IntentDeterminationTransformer_;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetConstraintTransformer(bool instantOnly, bool subGraph) override {
        Y_UNUSED(instantOnly);
        return subGraph ? *SubConstraintTransformer_ : *ConstraintTransformer_;
    }

    IGraphTransformer& GetLogicalOptProposalTransformer() override {
        return *LogicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalOptProposalTransformer() override {
        return *PhysicalOptProposalTransformer_;
    }

    IGraphTransformer& GetPhysicalFinalizingTransformer() override {
        return *PhysicalFinalizingTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

    IGraphTransformer& GetFinalizingTransformer() override {
        return *FinalizingTransformer_;
    }

    bool CollectStatistics(NYson::TYsonWriter& writer, bool totalOnly) override {
        if (State_->Statistics.empty()) {
            return false;
        }

        writer.OnBeginMap();
            NCommon::WriteStatistics(writer, totalOnly, State_->Statistics, true, false);
            writer.OnKeyedItem("HybridTotal");
            writer.OnBeginMap();
            for (const auto& [subFolder, stats] : State_->HybridStatistics) {
                if (subFolder.empty()) {
                    NCommon::WriteStatistics(writer, totalOnly, {{0, stats}}, false, false);
                } else {
                    writer.OnKeyedItem(subFolder);
                    NCommon::WriteStatistics(writer, totalOnly, {{0, stats}}, false);
                }
            }
            writer.OnEndMap();
            writer.OnKeyedItem("Hybrid");
            writer.OnBeginMap();
            for (const auto& [opName, hybridStats] : State_->HybridOpStatistics) {
                writer.OnKeyedItem(opName);
                writer.OnBeginMap();
                for (const auto& [subFolder, stats] : hybridStats) {
                    if (subFolder.empty()) {
                        NCommon::WriteStatistics(writer, totalOnly, {{0, stats}}, false, false);
                    } else {
                        writer.OnKeyedItem(subFolder);
                        NCommon::WriteStatistics(writer, totalOnly, {{0, stats}}, false);
                    }
                }
                writer.OnEndMap();
            }
            writer.OnEndMap();
        writer.OnEndMap();

        return true;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (!EnsureArgsCount(node, 2, ctx)) {
                return false;
            }

            if (node.Child(0)->Content() == YtProviderName) {
                if (!node.Child(1)->IsCallable("EvaluateAtom")) {
                    if (!EnsureAtom(*node.Child(1), ctx)) {
                        return false;
                    }

                    if (node.Child(1)->Content().empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), "Empty cluster name"));
                        return false;
                    }

                    cluster = TString(node.Child(1)->Content());
                }

                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Yt DataSink parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoWrite::CallableName())) {
            return TYtDSink::Match(node.Child(1));
        }

        return TypeAnnotationTransformer_->CanParse(node);
    }

    void FillModifyCallables(THashSet<TStringBuf>& callables) override {
        callables.insert(TYtWriteTable::CallableName());
        callables.insert(TYtDropTable::CallableName());
        callables.insert(TYtConfigure::CallableName());
    }

    bool IsWrite(const TExprNode& node) override {
        return TYtWriteTable::Match(&node);
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        YQL_ENSURE(TMaybeNode<TYtWrite>(node).DataSink());
        auto mode = NYql::GetSetting(*node->Child(4), EYtSettingType::Mode);
        if (mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Drop) {
            if (!node->Child(3)->IsCallable("Void")) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Child(3)->Pos()), TStringBuilder()
                    << "Expected Void, but got: " << node->Child(3)->Content()));
                return {};
            }

            TExprNode::TListType children = node->ChildrenList();
            children.resize(3);
            return ctx.NewCallable(node->Pos(), TYtDropTable::CallableName(), std::move(children));
        } else {
            auto res = ctx.RenameNode(*node, TYtWriteTable::CallableName());
            if ((!mode || FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Renew) && NYql::HasSetting(*node->Child(4), EYtSettingType::KeepMeta)) {
                auto settings = NYql::AddSetting(
                    *NYql::RemoveSettings(*node->Child(4), EYtSettingType::Mode | EYtSettingType::KeepMeta, ctx),
                    EYtSettingType::Mode,
                    ctx.NewAtom(node->Child(4)->Pos(), ToString(EYtWriteMode::RenewKeepMeta), TNodeFlags::ArbitraryContent),
                    ctx);
                res = ctx.ChangeChild(*res, TYtWriteTable::idx_Settings, std::move(settings));
            }
            if (auto columnGroup = NYql::GetSetting(*res->Child(TYtWriteTable::idx_Settings), EYtSettingType::ColumnGroups)) {
                const TString normalized = NormalizeColumnGroupSpec(columnGroup->Tail().Content());
                res = ctx.ChangeChild(*res, TYtWriteTable::idx_Settings,
                    NYql::UpdateSettingValue(*res->Child(TYtWriteTable::idx_Settings),
                        EYtSettingType::ColumnGroups,
                        ctx.NewAtom(res->Child(TYtWriteTable::idx_Settings)->Pos(), normalized, TNodeFlags::MultilineContent),
                        ctx)
                    );
            } else if (NYql::HasSetting(*res->Child(TYtWriteTable::idx_Table)->Child(TYtTable::idx_Settings), EYtSettingType::Anonymous)) {
                if (const auto mode = State_->Configuration->ColumnGroupMode.Get().GetOrElse(EColumnGroupMode::Disable); mode != EColumnGroupMode::Disable) {
                    res = ctx.ChangeChild(*res, TYtWriteTable::idx_Settings,
                        NYql::AddSetting(*res->Child(TYtWriteTable::idx_Settings),
                            EYtSettingType::ColumnGroups,
                            ctx.NewAtom(res->Child(TYtWriteTable::idx_Settings)->Pos(), NYql::GetSingleColumnGroupSpec(), TNodeFlags::MultilineContent),
                            ctx)
                        );
                }
            }
            auto mutationId = ++NextMutationId_;
            res = ctx.ChangeChild(*res, TYtWriteTable::idx_Settings,
                NYql::AddSetting(*res->Child(TYtWriteTable::idx_Settings),
                    EYtSettingType::MutationId,
                    ctx.NewAtom(res->Child(TYtWriteTable::idx_Settings)->Pos(), mutationId),
                    ctx)
                );
            if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
                res = ctx.ChangeChild(*res, TYtWriteTable::idx_Content,
                    ctx.Builder(node->Pos())
                        .Callable("RemovePrefixMembers")
                            .Add(0, node->ChildPtr(TYtWriteTable::idx_Content))
                            .List(1)
                                .Atom(0, YqlSysColumnPrefix)
                            .Seal()
                        .Seal()
                        .Build()
                    );
            }
            return res;
        }
    }

    void PostRewriteIO() final {
        if (!State_->Types->EvaluationInProgress) {
            State_->TablesData->CleanupCompiledSQL();
        }
    }

    void Reset() final {
        TDataProviderBase::Reset();
        State_->Reset();
        NextMutationId_ = 0;
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecTransformer_->CanExec(node);
    }

    bool ValidateExecution(const TExprNode& node, TExprContext& ctx) override {
        if (TYtDqProcessWrite::Match(&node)) {
            auto dqProvider = State_->Types->DataSourceMap.FindPtr(DqProviderName);
            YQL_ENSURE(dqProvider);
            return (*dqProvider)->ValidateExecution(TYtDqProcessWrite(&node).Input().Ref(), ctx);
        }
        return true;
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            if (TYtTransientOpBase::Match(&node)) {
                children.push_back(node.ChildPtr(TYtTransientOpBase::idx_Input));
            } else if (TYtPublish::Match(&node)) {
                children.push_back(node.ChildPtr(TYtPublish::idx_Input));
            } else if (TYtStatOut::Match(&node)) {
                children.push_back(node.ChildPtr(TYtStatOut::idx_Input));
            }
        }
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.emplace_back(node.HeadPtr());

            if (TMaybeNode<TYtOutputOpBase>(&node)) {
                for (size_t i = TYtOutputOpBase::idx_Output + 1; i < node.ChildrenSize(); ++i) {
                    ScanPlanDependencies(node.ChildPtr(i), children);
                }
            } else if (TMaybeNode<TYtPublish>(&node)) {
                ScanPlanDependencies(node.ChildPtr(TYtPublish::idx_Input), children);
            } else if (TMaybeNode<TYtStatOut>(&node)) {
                ScanPlanDependencies(node.ChildPtr(TYtStatOut::idx_Input), children);
            }

            return !TYtDqProcessWrite::Match(&node);
        }

        return false;
    }

    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override {
        if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(&node)) {
            writer.OnKeyedItem("InputColumns");
            auto op = maybeOp.Cast();
            if (op.Input().Size() > 1) {
                writer.OnBeginList();
                for (auto section: op.Input()) {
                    writer.OnListItem();
                    WriteColumns(writer, section.Paths().Item(0).Columns());
                }
                writer.OnEndList();
            }
            else {
                WriteColumns(writer, op.Input().Item(0).Paths().Item(0).Columns());
            }

            if (op.Input().Size() > 1) {
                writer.OnKeyedItem("InputSections");
                auto op = maybeOp.Cast();
                writer.OnBeginList();
                ui64 ndx = 0;
                for (auto section: op.Input()) {
                    writer.OnListItem();
                    writer.OnBeginList();
                    for (ui32 i = 0; i < Min((ui32)section.Paths().Size(), (withLimits && State_->PlanLimits) ? State_->PlanLimits : Max<ui32>()); ++i) {
                        writer.OnListItem();
                        writer.OnUint64Scalar(ndx++);
                    }
                    writer.OnEndList();
                }
                writer.OnEndList();
            }

            if (op.Maybe<TYtMap>() || op.Maybe<TYtMapReduce>() || op.Maybe<TYtMerge>() ||
                op.Maybe<TYtReduce>() || op.Maybe<TYtSort>() || op.Maybe<TYtEquiJoin>())
            {
                TSet<TString> keyFilterColumns;
                for (auto section: op.Input()) {
                    for (auto col : GetKeyFilterColumns(section, EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2)) {
                        keyFilterColumns.insert(TString(col));
                    }
                    for (auto path: section.Paths()) {
                        size_t keyLength = 0;
                        if (!path.Ranges().Maybe<TCoVoid>()) {
                            for (auto item: path.Ranges().Cast<TExprList>()) {
                                if (auto keyExact = item.Maybe<TYtKeyExact>()) {
                                    keyLength = Max(keyLength, keyExact.Cast().Key().Size());
                                } else if (auto keyRange = item.Maybe<TYtKeyRange>()) {
                                    keyLength = Max(keyLength, keyRange.Cast().Lower().Size(), keyRange.Cast().Upper().Size());
                                }
                            }
                        }
                        if (keyLength) {
                            auto rowSpec = TYtTableBaseInfo::GetRowSpec(path.Table());
                            YQL_ENSURE(rowSpec);
                            YQL_ENSURE(keyLength <= rowSpec->SortedBy.size());
                            keyFilterColumns.insert(rowSpec->SortedBy.begin(), rowSpec->SortedBy.begin() + keyLength);
                        }
                    }
                }

                if (!keyFilterColumns.empty()) {
                    writer.OnKeyedItem("InputKeyFilterColumns");
                    writer.OnBeginList();
                    for (auto column : keyFilterColumns) {
                        writer.OnListItem();
                        writer.OnStringScalar(column);
                    }
                    writer.OnEndList();
                }
            }

            static const EYtSettingType specialSettings[] = {EYtSettingType::FirstAsPrimary, EYtSettingType::JoinReduce};
            if (AnyOf(specialSettings, [&op](const auto& setting) { return NYql::HasSetting(op.Settings().Ref(), setting); })) {
                writer.OnKeyedItem("Settings");
                writer.OnBeginMap();

                for (auto setting: specialSettings) {
                    if (NYql::HasSetting(op.Settings().Ref(), setting)) {
                        writer.OnKeyedItem(ToString(setting));
                        writer.OnStringScalar("true");
                    }
                }

                if (NYql::UseJoinReduceForSecondAsPrimary(op.Settings().Ref())) {
                    writer.OnKeyedItem(NYql::JoinReduceForSecondAsPrimaryName);
                    writer.OnStringScalar("true");
                }

                writer.OnEndMap();
            }
        }

        if (auto maybeOp = TMaybeNode<TYtMap>(&node)) {
            writer.OnKeyedItem("Streams");
            writer.OnBeginMap();
            NCommon::WriteStreams(writer, "Mapper", maybeOp.Cast().Mapper());
            writer.OnEndMap();
        } else if (auto maybeOp = TMaybeNode<TYtReduce>(&node)) {
            writer.OnKeyedItem("Streams");
            writer.OnBeginMap();
            NCommon::WriteStreams(writer, "Reducer", maybeOp.Cast().Reducer());
            writer.OnEndMap();
        } else if (auto maybeOp = TMaybeNode<TYtMapReduce>(&node)) {
            writer.OnKeyedItem("Streams");
            writer.OnBeginMap();
            if (auto maybeLambda = maybeOp.Cast().Mapper().Maybe<TCoLambda>()) {
                NCommon::WriteStreams(writer, "Mapper", maybeLambda.Cast());
            }

            NCommon::WriteStreams(writer, "Reducer", maybeOp.Cast().Reducer());
            writer.OnEndMap();
        }
    }

    TString GetProviderPath(const TExprNode& node) override {
        return TStringBuilder() << YtProviderName << '.' << node.Child(1)->Content();
    }

    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("Cluster");
        writer.OnStringScalar(node.Child(1)->Content());
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimits) override {
        ui32 count = 0;
        if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(&node)) {
            auto op = maybeOp.Cast();
            for (auto section: op.Input()) {
                ui32 i = 0;
                for (auto path: section.Paths()) {
                    if (auto maybeTable = path.Table().Maybe<TYtTable>()) {
                        inputs.push_back(TPinInfo(nullptr, op.DataSink().Raw(), path.Raw(), MakeTableDisplayName(maybeTable.Cast(), false), false));
                    }
                    else {
                        auto tmpTable = GetOutTable(path.Table());
                        inputs.push_back(TPinInfo(nullptr, op.DataSink().Raw(), tmpTable.Raw(), MakeTableDisplayName(tmpTable, false), true));
                    }
                    if (withLimits && State_->PlanLimits && ++i >= State_->PlanLimits) {
                        break;
                    }
                }
                count += section.Paths().Size();
            }
        }
        else if (auto maybePublish = TMaybeNode<TYtPublish>(&node)) {
            auto publish = maybePublish.Cast();
            ui32 i = 0;
            for (auto out: publish.Input()) {
                auto tmpTable = GetOutTable(out);
                inputs.push_back(TPinInfo(nullptr, publish.DataSink().Raw(), tmpTable.Raw(), MakeTableDisplayName(tmpTable, false), true));
                if (withLimits && State_->PlanLimits && ++i >= State_->PlanLimits) {
                    break;
                }
            }
            count = publish.Input().Size();
        } else if (auto maybeStatOut = TMaybeNode<TYtStatOut>(&node)) {
            auto statOut = maybeStatOut.Cast();
            auto table = GetOutTable(statOut.Input());
            inputs.push_back(TPinInfo(nullptr, statOut.DataSink().Raw(), table.Raw(), MakeTableDisplayName(table, false), true));
            count = 1;
        }
        return count;
    }

    ui32 GetOutputs(const TExprNode& node, TVector<TPinInfo>& outputs, bool withLimits) override {
        Y_UNUSED(withLimits);
        ui32 count = 0;
        if (auto maybeOp = TMaybeNode<TYtOutputOpBase>(&node)) {
            auto op = maybeOp.Cast();
            for (auto table: op.Output()) {
                outputs.push_back(TPinInfo(nullptr, op.DataSink().Raw(), table.Raw(), MakeTableDisplayName(table, true), true));
            }
            count = op.Output().Size();
        }
        else if (auto maybePublish = TMaybeNode<TYtPublish>(&node)) {
            auto publish = maybePublish.Cast();
            outputs.push_back(TPinInfo(nullptr, publish.DataSink().Raw(), publish.Publish().Raw(), MakeTableDisplayName(publish.Publish(), true), false));
            count = 1;
        } else if (auto maybeStatOut = TMaybeNode<TYtStatOut>(&node)) {
            auto statOut = maybeStatOut.Cast();
            auto statTable = statOut.Table();
            outputs.push_back(TPinInfo(nullptr, statOut.DataSink().Raw(), statTable.Raw(), "(tmp)", true));
            count = 1;
        } else if (auto maybeWrite = TMaybeNode<TYtWriteTable>(&node)) {
            auto write = maybeWrite.Cast();
            outputs.push_back(TPinInfo(nullptr, write.DataSink().Raw(), write.Table().Raw(), MakeTableDisplayName(write.Table(), true), true));
            count = 1;
        }
        return count;
    }

    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("Table");
        if (auto path = TMaybeNode<TYtPath>(&node)) {
            const auto& table = path.Cast().Table().Cast<TYtTable>();
            writer.OnStringScalar(table.Name().Value());
            writer.OnKeyedItem("Type");
            auto rowType = path.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            NCommon::WriteTypeToYson(writer, rowType);
        } else if (auto table = TMaybeNode<TYtTable>(&node)) {
            writer.OnStringScalar(table.Cast().Name().Value());
            const auto tableInfo = TYtTableInfo(table.Cast());
            auto& desc = State_->TablesData->GetTable(tableInfo.Cluster, tableInfo.Name, tableInfo.CommitEpoch);
            if (desc.RowSpec) {
                writer.OnKeyedItem("Type");
                auto rowType = desc.RowSpec->GetType();
                NCommon::WriteTypeToYson(writer, rowType);
            }
        } else {
            writer.OnStringScalar("(tmp)");
        }
    }

    TString GetOperationDisplayName(const TExprNode& node) override {
        if (auto maybeCommit = TMaybeNode<TCoCommit>(&node)) {
            auto commit = maybeCommit.Cast();

            TStringBuilder res;
            res << node.Content() << " on " << commit.DataSink().Cast<TYtDSink>().Cluster().Value();
            if (commit.Settings()) {
                if (auto epochNode = NYql::GetSetting(commit.Settings().Cast().Ref(), "epoch")) {
                    res << " #" << epochNode->Child(1)->Content();
                }
            }
            return res;
        }

        return TString{node.Content()};
    }

    bool WriteSchemaHeader(NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("YtSchema");
        return true;
    }

    void WriteTypeDetails(NYson::TYsonWriter& writer, const TTypeAnnotationNode& type) override {
        writer.OnStringScalar(GetTypeV3String(type));
    }

    ITrackableNodeProcessor& GetTrackableNodeProcessor() override {
        return *TrackableNodeProcessor_;
    }

    IDqIntegration* GetDqIntegration() override {
        return State_->DqIntegration_.Get();
    }

private:
    static void WriteColumns(NYson::TYsonWriter& writer, TExprBase columns) {
        if (auto maybeList = columns.Maybe<TExprList>()) {
            writer.OnBeginList();
            for (const auto& column : maybeList.Cast()) {
                writer.OnListItem();
                if (auto atom = column.Maybe<TCoAtom>()) {
                    writer.OnStringScalar(atom.Cast().Value());
                }
                else {
                    writer.OnStringScalar(column.Cast<TExprList>().Item(0).Cast<TCoAtom>().Value());
                }
            }
            writer.OnEndList();
        } else if (columns.Maybe<TCoVoid>()) {
            writer.OnStringScalar("*");
        } else {
            writer.OnStringScalar("?");
        }
    }

private:
    ui32 NextMutationId_ = 0;
    TYtState::TPtr State_;
    TLazyInitHolder<IGraphTransformer> IntentDeterminationTransformer_;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    TLazyInitHolder<IGraphTransformer> ConstraintTransformer_;
    TLazyInitHolder<IGraphTransformer> SubConstraintTransformer_;
    TLazyInitHolder<TExecTransformerBase> ExecTransformer_;
    TLazyInitHolder<IGraphTransformer> LogicalOptProposalTransformer_;
    TLazyInitHolder<IGraphTransformer> PhysicalOptProposalTransformer_;
    TLazyInitHolder<IGraphTransformer> PhysicalFinalizingTransformer_;
    TLazyInitHolder<IGraphTransformer> FinalizingTransformer_;
    TLazyInitHolder<ITrackableNodeProcessor> TrackableNodeProcessor_;
};

TIntrusivePtr<IDataProvider> CreateYtDataSink(TYtState::TPtr state) {
    return new TYtDataSink(state);
}

}
