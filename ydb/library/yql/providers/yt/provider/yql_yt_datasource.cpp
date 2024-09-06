#include "yql_yt_provider.h"
#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_key.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_dq_integration.h"
#include "yql_yt_dq_optimize.h"

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_lazy_init.h>
#include <ydb/library/yql/providers/common/config/yql_configuration_transformer.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYql {

using namespace NNodes;

class TYtDataSourceTrackableNodeProcessor : public TTrackableNodeProcessorBase {
public:
    TYtDataSourceTrackableNodeProcessor(bool collectNodes)
        : CollectNodes(collectNodes)
    {
    }

    void GetUsedNodes(const TExprNode& input, TVector<TString>& usedNodeIds) override {
        usedNodeIds.clear();
        if (!CollectNodes) {
            return;
        }

        if (auto maybeResPull = TMaybeNode<TResPull>(&input)) {
            ScanForUsedOutputTables(maybeResPull.Cast().Data().Ref(), usedNodeIds);
        } else if (auto maybeResFill = TMaybeNode<TResFill>(&input)){
            ScanForUsedOutputTables(maybeResFill.Cast().Data().Ref(), usedNodeIds);
        } else if (auto maybeResIf = TMaybeNode<TResIf>(&input)){
            ScanForUsedOutputTables(maybeResIf.Cast().Condition().Ref(), usedNodeIds);
        } else if (TMaybeNode<TYtReadTable>(&input)) {
            ScanForUsedOutputTables(*input.Child(TYtReadTable::idx_Input), usedNodeIds);
        } else if (TMaybeNode<TYtReadTableScheme>(&input)) {
            ScanForUsedOutputTables(*input.Child(TYtReadTableScheme::idx_Type), usedNodeIds);
        }
    }

private:
    const bool CollectNodes;
};

namespace {

const THashSet<TStringBuf> CONFIGURE_CALLABLES = {
    TCoConfigure::CallableName(),
    TYtConfigure::CallableName(),
};

}

class TYtDataSource : public TDataProviderBase {
public:
    TYtDataSource(TYtState::TPtr state)
        : State_(state)
        , ConfigurationTransformer_([this]() {
            return MakeHolder<NCommon::TProviderConfigurationTransformer>(State_->Configuration, *State_->Types, TString{YtProviderName}, CONFIGURE_CALLABLES);
        })
        , IODiscoveryTransformer_([this]() { return CreateYtIODiscoveryTransformer(State_); })
        , EpochTransformer_([this]() { return CreateYtEpochTransformer(State_); })
        , IntentDeterminationTransformer_([this]() { return CreateYtIntentDeterminationTransformer(State_); })
        , LoadMetaDataTransformer_([this]() { return CreateYtLoadTableMetadataTransformer(State_); })
        , TypeAnnotationTransformer_([this]() { return CreateYtDataSourceTypeAnnotationTransformer(State_); })
        , ConstraintTransformer_([this]() { return CreateYtDataSourceConstraintTransformer(State_); })
        , ExecTransformer_([this]() { return CreateYtDataSourceExecTransformer(State_); })
        , TrackableNodeProcessor_([this]() {
            auto mode = GetReleaseTempDataMode(*State_->Configuration);
            bool collectNodes = mode == EReleaseTempDataMode::Immediate;
            return MakeHolder<TYtDataSourceTrackableNodeProcessor>(collectNodes);
        })
        , DqOptimizer_([this]() { return CreateYtDqOptimizers(State_); })
    {
    }

    void AddCluster(const TString& name, const THashMap<TString, TString>& properties) override {
        const TString& token = properties.Value("token", "");

        State_->Configuration->AddValidCluster(name);
        if (token) {
            // Empty token is forbidden for yt reader
            State_->Configuration->Tokens[name] = ComposeStructuredTokenJsonForTokenAuthWithSecret(properties.Value("tokenReference", ""), token);
        }

        TYtClusterConfig cluster;
        cluster.SetName(name);
        cluster.SetCluster(properties.Value("location", ""));
        cluster.SetYTToken(token);
        State_->Gateway->AddCluster(cluster);
    }

    TStringBuf GetName() const override {
        return YtProviderName;
    }

    IGraphTransformer& GetConfigurationTransformer() override {
        return *ConfigurationTransformer_;
    }

    IGraphTransformer& GetIODiscoveryTransformer() override {
        return *IODiscoveryTransformer_;
    }

    IGraphTransformer& GetEpochsTransformer() override {
        return *EpochTransformer_;
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
        Y_UNUSED(subGraph);
        return *ConstraintTransformer_;
    }

    IGraphTransformer& GetLoadTableMetadataTransformer() override {
        return *LoadMetaDataTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecTransformer_;
    }

    bool Initialize(TExprContext& ctx) override {
        auto category = YtProviderName;
        auto cred = State_->Types->Credentials->FindCredential(TString("default_").append(category));
        if (cred) {
            if (cred->Category != category) {
                ctx.AddError(TIssue({}, TStringBuilder()
                    << "Mismatch default credential category, expected: " << category
                    << ", but found: " << cred->Category));
                return false;
            }
            State_->Configuration->Auth = cred->Content;
        }

        return true;
    }

    const THashMap<TString, TString>* GetClusterTokens() override {
        return &State_->Configuration->Tokens;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
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

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Yt DataSource parameters"));
        return false;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return TYtDSource::Match(node.Child(1));
        }

        return TypeAnnotationTransformer_->CanParse(node);
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        YQL_CLOG(INFO, ProviderYt) << "RewriteIO";

        auto read = TCoInputBase(node).Input().Cast<TYtRead>();
        bool buildLeft = TCoLeft::Match(node.Get());
        bool buildReadTableScheme = NYql::HasSetting(read.Arg(4).Ref(), EYtSettingType::Scheme);

        if (buildLeft && (buildReadTableScheme || !State_->PassiveExecution)) {
            return read.World().Ptr();
        }

        if (buildReadTableScheme) {
            YQL_ENSURE(read.Arg(2).Maybe<TExprList>().Item(0).Maybe<TYtPath>());

            auto newRead = InjectUdfRemapperOrView(read, ctx, true, false);

            return Build<TCoRight>(ctx, node->Pos())
                .Input<TYtReadTableScheme>()
                    .World(read.World())
                    .DataSource(read.DataSource())
                    .Table(read.Arg(2).Cast<TExprList>().Item(0).Cast<TYtPath>().Table().Cast<TYtTable>())
                    .Type<TCoTypeOf>()
                        .Value(newRead)
                    .Build()
                .Build()
                .Done().Ptr();
        }

        YQL_ENSURE(read.Arg(2).Maybe<TExprList>().Item(0).Maybe<TYtPath>()); // At least one table
        return InjectUdfRemapperOrView(read, ctx, false, buildLeft);
    }

    void PostRewriteIO() final {
        if (!State_->Types->EvaluationInProgress) {
            State_->TablesData->CleanupCompiledSQL();
        }
    }

    void Reset() final {
        TDataProviderBase::Reset();
        State_->TablesData = MakeIntrusive<TYtTablesData>();
        State_->Configuration->ClearVersions();
        State_->NodeHash.clear();
        State_->Checkpoints.clear();
    }

    void EnterEvaluation(ui64 id) final {
        State_->EnterEvaluation(id);
    }

    void LeaveEvaluation(ui64 id) final {
        State_->LeaveEvaluation(id);
    }

    bool IsPersistent(const TExprNode& node) override {
        return IsYtProviderInput(NNodes::TExprBase(&node));
    }

    bool IsRead(const TExprNode& node) override {
        return TYtReadTable::Match(&node);
    }

    bool CanBuildResult(const TExprNode& node, TSyncMap& syncList) override {
        TString usedCluster;
        return IsYtCompleteIsolatedLambda(node, syncList, usedCluster, false);
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        auto read = TMaybeNode<TYtReadTable>(node);
        if (!read) {
            return false;
        }

        root = read.Cast().World().Ptr();
        return true;
    }

    bool CanEvaluate(const TExprNode& node) override {
        return TYtConfigure::Match(&node);
    }

    bool CanPullResult(const TExprNode& node, TSyncMap& syncList, bool& canRef) override {
        Y_UNUSED(syncList);
        canRef = true;
        auto input = NNodes::TExprBase(&node);
        return IsYtProviderInput(input) || input.Maybe<TCoRight>().Input().Maybe<TYtReadTableScheme>();
    }

    TExprNode::TPtr CleanupWorld(const TExprNode::TPtr& node, TExprContext& ctx) override {
        return YtCleanupWorld(node, ctx, State_);
    }

    TExprNode::TPtr OptimizePull(const TExprNode::TPtr& source, const TFillSettings& fillSettings, TExprContext& ctx,
        IOptimizationContext& optCtx) override
    {
        Y_UNUSED(optCtx);

        auto maybeRight = TMaybeNode<TCoRight>(source);
        if (!maybeRight || !fillSettings.RowsLimitPerWrite) {
            return source;
        }

        auto maybeRead = maybeRight.Input().Maybe<TYtReadTable>();
        if (!maybeRead) {
            return source;
        }
        auto read = maybeRead.Cast();
        if (read.Input().Size() > 1) {
            return source;
        }
        auto section = read.Input().Item(0);
        ui64 totalCount = 0;
        for (auto path: section.Paths()) {
            TYtTableBaseInfo::TPtr tableInfo = TYtTableBaseInfo::Parse(path.Table());
            if (!tableInfo->Meta || !tableInfo->Stat || tableInfo->Meta->IsDynamic) {
                totalCount = Max<ui64>();
                break;
            }
            totalCount += tableInfo->Stat->RecordsCount;
        }

        if (totalCount <= *fillSettings.RowsLimitPerWrite) {
            return source;
        }

        auto newSettings = NYql::AddSetting(
            section.Settings().Ref(),
            EYtSettingType::Take,
            Build<TCoUint64>(ctx, read.Pos())
                .Literal()
                    .Value(ToString(*fillSettings.RowsLimitPerWrite))
                .Build()
            .Done().Ptr(),
            ctx);

        return Build<TCoRight>(ctx, maybeRight.Cast().Pos())
            .Input<TYtReadTable>()
                .World(read.World())
                .DataSource(read.DataSource())
                .Input()
                    .Add()
                        .Paths(section.Paths())
                        .Settings(newSettings)
                    .Build()
                .Build()
            .Build()
            .Done().Ptr();
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecTransformer_->CanExec(node);
    }

    void GetRequiredChildren(const TExprNode& node, TExprNode::TListType& children) override {
        if (CanExecute(node) && !TYtTable::Match(&node)) {
            children.push_back(node.ChildPtr(0));
            if (TYtReadTable::Match(&node)) {
                children.push_back(node.ChildPtr(TYtReadTable::idx_Input));
            }
        }
    }

    bool GetDependencies(const TExprNode& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        if (CanExecute(node)) {
            children.push_back(node.ChildPtr(0));
            if (node.Content() == TYtConfigure::CallableName()) {
                return false;
            }

            if (TMaybeNode<TYtReadTable>(&node)) {
                ScanPlanDependencies(node.ChildPtr(TYtReadTable::idx_Input), children);
            }

            if (TMaybeNode<TYtReadTableScheme>(&node)) {
                ScanPlanDependencies(node.ChildPtr(TYtReadTableScheme::idx_Type), children);
            }

            return true;
        }

        return false;
    }

    void GetResultDependencies(const TExprNode::TPtr& node, TExprNode::TListType& children, bool compact) override {
        Y_UNUSED(compact);
        ScanPlanDependencies(node, children);
    }

    void WritePlanDetails(const TExprNode& node, NYson::TYsonWriter& writer, bool withLimits) override {
        if (auto maybeRead = TMaybeNode<TYtReadTable>(&node)) {
            writer.OnKeyedItem("InputColumns");
            auto read = maybeRead.Cast();
            if (read.Input().Size() > 1) {
                writer.OnBeginList();
                for (auto section: read.Input()) {
                    writer.OnListItem();
                    NCommon::WriteColumns(writer, section.Paths().Item(0).Columns());
                }
                writer.OnEndList();
            }
            else {
                NCommon::WriteColumns(writer, read.Input().Item(0).Paths().Item(0).Columns());
            }

            if (read.Input().Size() > 1) {
                writer.OnKeyedItem("InputSections");
                writer.OnBeginList();
                ui64 ndx = 0;
                for (auto section: read.Input()) {
                    writer.OnListItem();
                    writer.OnBeginList();
                    for (ui32 i = 0; i < Min((ui32)section.Paths().Size(), withLimits && State_->PlanLimits ? State_->PlanLimits : Max<ui32>()); ++i) {
                        writer.OnListItem();
                        writer.OnUint64Scalar(ndx++);
                    }
                    writer.OnEndList();
                }
                writer.OnEndList();
            }
        }
    }

    void WritePullDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("PullOperation");
        writer.OnStringScalar(node.Child(0)->Content());
    }

    TString GetProviderPath(const TExprNode& node) override {
        return TStringBuilder() << YtProviderName << '.' << node.Child(1)->Content();
    }

    void WriteDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("Cluster");
        writer.OnStringScalar(node.Child(1)->Content());
    }

    ui32 GetInputs(const TExprNode& node, TVector<TPinInfo>& inputs, bool withLimit) override {
        ui32 count = 0;
        if (auto maybeRead = TMaybeNode<TYtReadTable>(&node)) {
            auto read = maybeRead.Cast();
            for (auto section: read.Input()) {
                ui32 i = 0;
                for (auto path: section.Paths()) {
                    if (auto maybeTable = path.Table().Maybe<TYtTable>()) {
                        inputs.push_back(TPinInfo(read.DataSource().Raw(), nullptr, maybeTable.Cast().Raw(), MakeTableDisplayName(maybeTable.Cast(), false), false));
                    }
                    else {
                        auto tmpTable = GetOutTable(path.Table());
                        inputs.push_back(TPinInfo(read.DataSource().Raw(), nullptr, tmpTable.Raw(), MakeTableDisplayName(tmpTable, false), true));
                    }
                    if (withLimit && State_->PlanLimits && ++i >= State_->PlanLimits) {
                        break;
                    }
                }
                count += section.Paths().Size();
            }
        }
        else if (auto maybeReadScheme = TMaybeNode<TYtReadTableScheme>(&node)) {
            auto readScheme = maybeReadScheme.Cast();
            inputs.push_back(TPinInfo(readScheme.DataSource().Raw(), nullptr, readScheme.Table().Raw(), MakeTableDisplayName(readScheme.Table(), false), false));
            count = 1;
        }
        return count;
    }

    void WritePinDetails(const TExprNode& node, NYson::TYsonWriter& writer) override {
        writer.OnKeyedItem("Table");
        if (auto table = TMaybeNode<TYtTable>(&node)) {
            writer.OnStringScalar(table.Cast().Name().Value());
        }
        else {
            writer.OnStringScalar("(tmp)");
        }
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

    bool CollectDiscoveredData(NYson::TYsonWriter& writer) override {
        THashMap<std::pair<TString, TString>, THashSet<TString>> tables;
        State_->TablesData->ForEach([&tables](const TString& cluster, const TString& table, ui32 /*epoch*/, const TYtTableDescription& tableDesc) {
            if (!tableDesc.IsAnonymous) {
                TStringBuf intent;
                if (tableDesc.Intents & TYtTableIntent::Drop) {
                    intent = "drop";
                } else if (tableDesc.Intents & (TYtTableIntent::Override | TYtTableIntent::Append)) {
                    intent = "modify";
                } else if (tableDesc.Intents & TYtTableIntent::Flush) {
                    intent = "flush";
                } else {
                    intent = "read";
                }
                tables[std::make_pair(cluster, table)].emplace(intent);
            }
        });
        if (tables.empty()) {
            return false;
        }
        writer.OnBeginList();
        for (auto& item: tables) {
            writer.OnListItem();

            writer.OnBeginList();
            writer.OnListItem();
            writer.OnStringScalar(item.first.first);
            writer.OnListItem();
            writer.OnStringScalar(item.first.second);
            writer.OnListItem();

            writer.OnBeginList();
            for (auto& intent: item.second) {
                writer.OnListItem();
                writer.OnStringScalar(intent);
            }
            writer.OnEndList();

            writer.OnEndList();
        }
        writer.OnEndList();
        return true;
    }

    IDqIntegration* GetDqIntegration() override {
        return State_->DqIntegration_.Get();
    }

    IDqOptimization* GetDqOptimization() override {
        return DqOptimizer_.Get();
    }

private:
    TExprNode::TPtr InjectUdfRemapperOrView(TYtRead readNode, TExprContext& ctx, bool fromReadSchema, bool buildLeft) {
        const bool weakConcat = NYql::HasSetting(readNode.Arg(4).Ref(), EYtSettingType::WeakConcat);
        const bool ignoreNonExisting = NYql::HasSetting(readNode.Arg(4).Ref(), EYtSettingType::IgnoreNonExisting);
        const bool warnNonExisting = NYql::HasSetting(readNode.Arg(4).Ref(), EYtSettingType::WarnNonExisting);
        const bool inlineContent = NYql::HasSetting(readNode.Arg(4).Ref(), EYtSettingType::Inline);
        const auto cleanReadSettings = NYql::RemoveSettings(readNode.Arg(4).Ref(),
            EYtSettingType::WeakConcat | EYtSettingType::Inline | EYtSettingType::Scheme | EYtSettingType::IgnoreNonExisting | EYtSettingType::WarnNonExisting, ctx);

        const bool useSysColumns = !fromReadSchema && State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS);
        bool hasSysColumns = false;

        auto cluster = TString{readNode.DataSource().Cluster().Value()};

        bool hasNonExisting = false;
        bool readAllFields = false;
        TSet<TString> subsetFields;
        TExprNode::TListType tableReads;
        for (auto node: readNode.Arg(2).Cast<TExprList>()) {
            TYtPath path = node.Cast<TYtPath>();
            const bool hasMultipleUserRanges = path.Ranges().Maybe<TExprList>() && path.Ranges().Cast<TExprList>().Size() > 1;
            TYtTable table = path.Table().Cast<TYtTable>();
            if (!path.Columns().Maybe<TCoVoid>() && !readNode.Arg(3).Maybe<TCoVoid>()) {
                ctx.AddError(TIssue(ctx.GetPosition(readNode.Pos()), TStringBuilder()
                    << "Column selection in both read and YPath for table " <<  table.Name().Value()));
                return {};
            }

            auto epoch = TEpochInfo::Parse(table.Epoch().Ref());
            const TYtTableDescription& tableDesc = State_->TablesData->GetTable(cluster, TString{table.Name().Value()}, epoch);

            auto tableMeta = tableDesc.Meta;
            auto tableStat = tableDesc.Stat;

            TExprBase origColumnList = path.Columns();
            if (!path.Columns().Maybe<TCoVoid>()) {
                for (auto child: path.Columns().Ref().Children()) {
                    subsetFields.insert(TString{child->Content()});
                }
            }
            else if (!readNode.Arg(3).Maybe<TCoVoid>()) {
                for (auto child: readNode.Arg(3).Ref().Children()) {
                    subsetFields.insert(TString{child->Content()});
                }
                origColumnList = readNode.Arg(3);
                YQL_ENSURE(origColumnList.Ref().IsList());
            }
            else {
                readAllFields = true;
            }

            bool skipTable = false;
            if (weakConcat
                && !tableDesc.View // Don't skip view
                && epoch.GetOrElse(0) == 0
                && tableStat && tableStat->IsEmpty()
                && tableMeta && !tableMeta->YqlCompatibleScheme) {
                // skip empty tables without YQL compatible scheme
                skipTable = true;
            }

            if (ignoreNonExisting
                && epoch.GetOrElse(0) == 0
                && tableMeta && !tableMeta->DoesExist) {
                // skip non-existing tables
                hasNonExisting = true;
                skipTable = true;
                if (warnNonExisting) {
                    auto issue = TIssue(ctx.GetPosition(table.Pos()), TStringBuilder()
                        << "Table " << cluster << '.' << table.Name().Value() << " doesn't exist");
                    SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_YT_WARN_TABLE_DOES_NOT_EXIST, issue);
                    if (!ctx.AddWarning(issue)) {
                        return {};
                    }
                }
            }

            if (skipTable) {
                auto userSchema = GetSetting(table.Settings().Ref(), EYtSettingType::UserSchema);
                if (userSchema) {
                    tableReads.push_back(ctx.Builder(table.Pos())
                        .Callable(buildLeft ? "Left!" : "Right!")
                            .Add(0, BuildEmptyTablesRead(table.Pos(), *userSchema, ctx))
                        .Seal()
                        .Build());
                }

                continue;
            }

            auto updatedSettings = NYql::RemoveSetting(table.Settings().Ref(), EYtSettingType::XLock, ctx);

            TString view;
            if (auto viewNode = NYql::GetSetting(*updatedSettings, EYtSettingType::View)) {
                view = TString{viewNode->Child(1)->Content()};
                // truncate view
                updatedSettings = NYql::RemoveSetting(*updatedSettings, EYtSettingType::View, ctx);
            }

            bool withQB = tableDesc.QB2RowSpec && view != "raw";
            if (withQB) {
                if (inlineContent) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Table with QB2 premapper cannot be inlined: " << cluster << '.' << table.Name().Value()));
                    return {};
                }
                if (tableDesc.RowSpec && 0 != tableDesc.RowSpec->GetNativeYtTypeFlags()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "Table with type_v3 schema cannot be used with QB2 premapper: " << cluster << '.' << table.Name().Value()));
                    return {};
                }
                updatedSettings = NYql::AddSetting(*updatedSettings, EYtSettingType::WithQB, {}, ctx);
            }

            table = Build<TYtTable>(ctx, table.Pos())
                .InitFrom(table)
                .Settings(updatedSettings)
                .Done();

            const bool needRewrite = weakConcat
                || tableDesc.UdfApplyLambda
                || !view.empty()
                || withQB;

            path = Build<TYtPath>(ctx, path.Pos())
                .InitFrom(path)
                .Table(table)
                .Columns(needRewrite ? Build<TCoVoid>(ctx, path.Columns().Pos()).Done() : origColumnList)
                .Done();

            bool tableSysColumns = useSysColumns && !tableDesc.View && (view.empty() || view == "raw");
            if (tableSysColumns) {
                hasSysColumns = true;
            }

            TExprNode::TPtr effectiveSettings = cleanReadSettings;
            if (tableSysColumns) {
                effectiveSettings = NYql::AddSettingAsColumnList(*effectiveSettings, EYtSettingType::SysColumns, {TString{"path"}, TString{"record"}}, ctx);
            }

            if (hasMultipleUserRanges) {
                effectiveSettings = NYql::AddSetting(*effectiveSettings, EYtSettingType::Unordered, nullptr, ctx);
                effectiveSettings = NYql::AddSetting(*effectiveSettings, EYtSettingType::NonUnique, nullptr, ctx);
            }

            auto origReadNode = Build<TYtReadTable>(ctx, readNode.Pos())
                .World(readNode.World())
                .DataSource(readNode.DataSource())
                .Input()
                    .Add()
                        .Paths()
                            .Add(path)
                        .Build()
                        .Settings(effectiveSettings)
                    .Build()
                .Build()
                .Done();

            if (buildLeft) {
                TExprNode::TPtr leftOverRead = Build<TCoLeft>(ctx, readNode.Pos())
                        .Input(origReadNode)
                        .Done().Ptr();

                tableReads.push_back(leftOverRead);
                continue;
            }

            TExprNode::TPtr rightOverRead = inlineContent
                ? Build<TYtTableContent>(ctx, readNode.Pos())
                    .Input(origReadNode)
                    .Settings().Build()
                    .Done().Ptr()
                : Build<TCoRight>(ctx, readNode.Pos())
                    .Input(origReadNode)
                    .Done().Ptr();

            TExprNode::TPtr newReadNode = rightOverRead;

            if (tableDesc.View) {
                auto root = tableDesc.View->CompiledSql;
                YQL_ENSURE(root);
                if (readNode.World().Ref().Type() != TExprNode::World) {
                    // Inject original Read! dependencies
                    auto status = OptimizeExpr(root, root, [&readNode](const TExprNode::TPtr& node, TExprContext& ctx) {
                        if (node->ChildrenSize() > 0 && node->Child(0)->Type() == TExprNode::World) {
                            return ctx.ChangeChild(*node, 0, readNode.World().Ptr());
                        }
                        return node;
                    }, ctx, TOptimizeExprSettings(nullptr));

                    if (status.Level == IGraphTransformer::TStatus::Error) {
                        return {};
                    }
                }

                newReadNode = root;
                ctx.Step
                    .Repeat(TExprStep::ExpandApplyForLambdas)
                    .Repeat(TExprStep::ExprEval)
                    .Repeat(TExprStep::DiscoveryIO)
                    .Repeat(TExprStep::Epochs)
                    .Repeat(TExprStep::Intents)
                    .Repeat(TExprStep::LoadTablesMetadata)
                    .Repeat(TExprStep::RewriteIO);
            }

            if (tableDesc.UdfApplyLambda) {
                if (tableSysColumns) {
                    newReadNode = ctx.Builder(readNode.Pos())
                        .Callable(ctx.IsConstraintEnabled<TSortedConstraintNode>() ? TCoOrderedMap::CallableName() : TCoMap::CallableName())
                            .Add(0, newReadNode)
                            .Lambda(1)
                                .Param("row")
                                .Callable("AddMember")
                                    .Callable(0, "AddMember")
                                        .Apply(0, tableDesc.UdfApplyLambda)
                                            .With(0)
                                                .Callable("RemovePrefixMembers")
                                                    .Arg(0, "row")
                                                    .List(1)
                                                        .Atom(0, YqlSysColumnPrefix, TNodeFlags::Default)
                                                    .Seal()
                                                .Seal()
                                            .Done()
                                        .Seal()
                                        .Atom(1, YqlSysColumnRecord, TNodeFlags::Default)
                                        .Callable(2, "Member")
                                            .Arg(0, "row")
                                            .Atom(1, YqlSysColumnRecord, TNodeFlags::Default)
                                        .Seal()
                                    .Seal()
                                    .Atom(1, YqlSysColumnPath, TNodeFlags::Default)
                                    .Callable(2, "Member")
                                        .Arg(0, "row")
                                        .Atom(1, YqlSysColumnPath, TNodeFlags::Default)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();
                } else {
                    newReadNode = ctx.Builder(readNode.Pos())
                        .Callable(ctx.IsConstraintEnabled<TSortedConstraintNode>() ? TCoOrderedMap::CallableName() : TCoMap::CallableName())
                            .Add(0, newReadNode)
                            .Add(1, tableDesc.UdfApplyLambda)
                        .Seal()
                        .Build();
                }
            }

            if (!view.empty()) {
                auto viewMeta = tableDesc.Views.FindPtr(view);
                YQL_ENSURE(viewMeta, "Unknown view: " << view);
                auto root = viewMeta->CompiledSql;
                YQL_ENSURE(root, "View is not initialized: " << view);
                TOptimizeExprSettings settings(nullptr);
                settings.VisitChanges = true;
                TExprNode::TListType innerWorlds;
                const bool enableViewIsolation = State_->Configuration->ViewIsolation.Get().GetOrElse(false);
                auto status = OptimizeExpr(root, root, [newReadNode, rightOverRead, &readNode, enableViewIsolation, &innerWorlds](const TExprNode::TPtr& node, TExprContext& ctx) {
                    if (auto world = TMaybeNode<TCoLeft>(node).Input().Maybe<TCoRead>().World()) {
                        return world.Cast().Ptr();
                    }

                    if (auto maybeRead = TMaybeNode<TCoRight>(node).Input().Maybe<TCoRead>()) {
                        TCoRead read = maybeRead.Cast();
                        TYtInputKeys keys;
                        if (!keys.Parse(read.Arg(2).Ref(), ctx)) {
                            return TExprNode::TPtr();
                        }
                        YQL_ENSURE(keys.GetKeys().size() == 1, "Expected single table name");
                        auto tableName = keys.GetKeys().front().GetPath();
                        TExprNode::TPtr selfRead;
                        if (tableName == TStringBuf("self")) {
                            selfRead = newReadNode;
                        } else if (tableName == TStringBuf("self_raw")) {
                            selfRead = rightOverRead;
                        } else {
                            YQL_ENSURE(false, "Unknown table name (should be self or self_raw): " << tableName);
                        }

                        if (enableViewIsolation) {
                            if (read.World().Raw()->Type() != TExprNode::World) {
                                innerWorlds.push_back(read.World().Ptr());
                            }
                        }

                        return selfRead;
                    }

                    // Inject original Read! dependencies
                    if (node->ChildrenSize() > 0 && node->Child(0)->Type() == TExprNode::World) {
                        return ctx.ChangeChild(*node, 0, readNode.World().Ptr());
                    }

                    return node;
                }, ctx, settings);

                if (status.Level == IGraphTransformer::TStatus::Error) {
                    return {};
                }

                if (!innerWorlds.empty()) {
                    innerWorlds.push_back(origReadNode.World().Ptr());
                    auto combined = ctx.NewCallable(origReadNode.World().Pos(), "Sync!", std::move(innerWorlds));
                    root = ctx.ReplaceNode(std::move(root), *origReadNode.World().Raw(), combined);
                }

                newReadNode = root;
                ctx.Step
                    .Repeat(TExprStep::ExpandApplyForLambdas)
                    .Repeat(TExprStep::ExprEval)
                    .Repeat(TExprStep::RewriteIO);
            }

            if (needRewrite && !weakConcat && !origColumnList.Maybe<TCoVoid>()) {
                TSet<TString> names;
                for (auto child: origColumnList.Ref().Children()) {
                    names.emplace(child->Content());
                }
                if (tableSysColumns) {
                    names.insert(TString{YqlSysColumnPath});
                    names.insert(TString{YqlSysColumnRecord});
                }
                newReadNode = FilterByFields(readNode.Pos(), newReadNode, names, ctx, false);
            }

            tableReads.push_back(newReadNode);
        }

        if (buildLeft) {
            return ctx.NewCallable(readNode.Pos(), TCoSync::CallableName(), std::move(tableReads));
        }

        if (tableReads.empty()) {
            if (hasNonExisting) {
                ctx.AddError(TIssue(ctx.GetPosition(readNode.Pos()), "The list of tables is empty"));
                return {};
            }

            return Build<TCoList>(ctx, readNode.Pos())
                .ListType<TCoListType>()
                    .ItemType<TCoStructType>()
                    .Build()
                .Build()
                .Done().Ptr();
        }

        TStringBuf extendName = ctx.IsConstraintEnabled<TSortedConstraintNode>()
            ? (weakConcat ? TCoUnionMerge::CallableName() : TCoMerge::CallableName())
            : (weakConcat ? TCoUnionAll::CallableName() : TCoExtend::CallableName());

        auto ret = ctx.NewCallable(readNode.Pos(), extendName, std::move(tableReads));

        if (weakConcat && !readAllFields) {
            if (hasSysColumns) {
                subsetFields.insert(TString{YqlSysColumnPath});
                subsetFields.insert(TString{YqlSysColumnRecord});
            }
            ret = FilterByFields(readNode.Pos(), ret, subsetFields, ctx, false);
        }

        return ret;
    }

private:
    TYtState::TPtr State_;
    TLazyInitHolder<IGraphTransformer> ConfigurationTransformer_;
    TLazyInitHolder<IGraphTransformer> IODiscoveryTransformer_;
    TLazyInitHolder<IGraphTransformer> EpochTransformer_;
    TLazyInitHolder<IGraphTransformer> IntentDeterminationTransformer_;
    TLazyInitHolder<IGraphTransformer> LoadMetaDataTransformer_;
    TLazyInitHolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    TLazyInitHolder<IGraphTransformer> ConstraintTransformer_;
    TLazyInitHolder<TExecTransformerBase> ExecTransformer_;
    TLazyInitHolder<TYtDataSourceTrackableNodeProcessor> TrackableNodeProcessor_;
    TLazyInitHolder<IDqOptimization> DqOptimizer_;
};

TIntrusivePtr<IDataProvider> CreateYtDataSource(TYtState::TPtr state) {
    return new TYtDataSource(state);
}

}
