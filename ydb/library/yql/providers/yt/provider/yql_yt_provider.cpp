#include "yql_yt_provider.h"
#include "yql_yt_dq_integration.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/activation/yql_activation.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/yt/gateway/qplayer/yql_yt_qplayer_gateway.h>

#include <util/generic/singleton.h>

namespace NYql {

bool TYtTableDescription::Fill(
    const TString& cluster, const TString& table, TExprContext& ctx,
    IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
    bool allowViewIsolation, IUdfResolver::TPtr udfResolver) {
    const TStructExprType* type = RowSpec ? RowSpec->GetType() : nullptr;
    if (!type) {
        TVector<const TItemExprType*> items;
        if (Meta->YqlCompatibleScheme) {
            for (auto& name : YAMR_FIELDS) {
                items.push_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TDataExprType>(EDataSlot::String)));
            }
        }
        type = ctx.MakeType<TStructExprType>(items);
    }

    if (!TYtTableDescriptionBase::Fill(TString{YtProviderName}, cluster,
        table, type, Meta->SqlView, Meta->SqlViewSyntaxVersion, Meta->Attrs, ctx,
        moduleResolver, urlListerManager, randomProvider, allowViewIsolation, udfResolver)) {
        return false;
    }
    if (QB2RowSpec) {
        RowType = QB2RowSpec->GetType();
    }
    return true;
}

void TYtTableDescription::ToYson(NYson::TYsonWriter& writer, const TString& cluster, const TString& table, const TString& view) const
{
    YQL_ENSURE(Stat);
    YQL_ENSURE(Meta);
    const bool isView = !view.empty() || View.Defined();
    const TYtViewDescription* viewMeta = !view.empty() ? Views.FindPtr(view) : View.Get();

    writer.OnBeginMap();
    writer.OnKeyedItem("Cluster");
    writer.OnStringScalar(cluster);
    writer.OnKeyedItem("Name");
    writer.OnStringScalar(table);
    if (isView) {
        YQL_ENSURE(viewMeta);
        writer.OnKeyedItem("View");
        writer.OnStringScalar(!view.empty() ? view : table);
        writer.OnKeyedItem("Sql");
        writer.OnStringScalar(viewMeta->Sql);
    } else {
        writer.OnKeyedItem("DoesExist");
        writer.OnBooleanScalar(Meta->DoesExist);
        writer.OnKeyedItem("IsEmpty");
        writer.OnBooleanScalar(Stat->IsEmpty());
        writer.OnKeyedItem("IsSorted");
        writer.OnBooleanScalar(RowSpec && RowSpec->IsSorted());
        writer.OnKeyedItem("IsDynamic");
        writer.OnBooleanScalar(Meta->IsDynamic);
        writer.OnKeyedItem("UniqueKeys");
        writer.OnBooleanScalar(RowSpec && RowSpec->UniqueKeys);
        writer.OnKeyedItem("CanWrite");
        writer.OnBooleanScalar(Meta->CanWrite);
        writer.OnKeyedItem("RecordsCount");
        writer.OnInt64Scalar(Stat->RecordsCount);
        writer.OnKeyedItem("DataSize");
        writer.OnInt64Scalar(Stat->DataSize);
        writer.OnKeyedItem("ChunkCount");
        writer.OnInt64Scalar(Stat->ChunkCount);
        writer.OnKeyedItem("ModifyTime");
        writer.OnInt64Scalar(Stat->ModifyTime);
        writer.OnKeyedItem("Id");
        writer.OnStringScalar(Stat->Id);
        writer.OnKeyedItem("Revision");
        writer.OnUint64Scalar(Stat->Revision);
        writer.OnKeyedItem("IsRealData");

        bool isRealData = !Meta->Attrs.contains(QB2Premapper) && !Meta->Attrs.contains(YqlReadUdfAttribute) && !IgnoreTypeV3;
        if (isRealData) {
            for (auto& x: Meta->Attrs) {
                if (x.first.StartsWith(YqlProtoFieldPrefixAttribute)) {
                    isRealData = false;
                    break;
                }
            }
        }
        writer.OnBooleanScalar(isRealData);
        writer.OnKeyedItem("YqlCompatibleSchema");
        writer.OnBooleanScalar(Meta->YqlCompatibleScheme);
    }

    const TTypeAnnotationNode* rowType = viewMeta
        ? viewMeta->RowType
        : QB2RowSpec
            ? QB2RowSpec->GetType()
            : RowType;
    auto rowSpec = QB2RowSpec ? QB2RowSpec : RowSpec;

     // fields

    auto writeSortOrder = [&writer](TMaybe<size_t> order, TMaybe<bool> ascending) {
        writer.OnKeyedItem("ClusterSortOrder");
        writer.OnBeginList();
        if (order) {
            writer.OnListItem();
            writer.OnInt64Scalar(*order);
        }
        writer.OnEndList();

        writer.OnKeyedItem("Ascending");
        writer.OnBeginList();
        if (ascending) {
            writer.OnListItem();
            writer.OnBooleanScalar(*ascending);
        }
        writer.OnEndList();
    };

    writer.OnKeyedItem("Fields");
    writer.OnBeginList();
    if ((isView || Meta->DoesExist) && rowType->GetKind() == ETypeAnnotationKind::Struct) {
        for (auto& item: rowType->Cast<TStructExprType>()->GetItems()) {
            writer.OnListItem();

            auto name = item->GetName();
            writer.OnBeginMap();

            writer.OnKeyedItem("Name");
            writer.OnStringScalar(name);

            writer.OnKeyedItem("Type");
            NCommon::WriteTypeToYson(writer, item->GetItemType());

            size_t fieldIdx = rowSpec && rowSpec->IsSorted()
                ? FindIndex(rowSpec->SortMembers, name)
                : NPOS;

            if (fieldIdx != NPOS) {
                bool ascending = !rowSpec->SortDirections.empty()
                    ? rowSpec->SortDirections.at(fieldIdx)
                    : true;
                writeSortOrder(fieldIdx, ascending);
            } else {
                writeSortOrder(Nothing(), Nothing());
            }
            writer.OnEndMap();
        }
    }
    writer.OnEndList();

    writer.OnKeyedItem("RowType");
    NCommon::WriteTypeToYson(writer, rowType);

    if (!isView) {
        // meta attr
        writer.OnKeyedItem("MetaAttr");
        writer.OnBeginMap();

        for (const auto& attr : Meta->Attrs) {
            if (attr.first.StartsWith("_yql")) {
                continue;
            }

            writer.OnKeyedItem(attr.first);
            writer.OnStringScalar(attr.second);
        }

        writer.OnEndMap();

        // views
        writer.OnKeyedItem("Views");
        TVector<TString> views;
        for (const auto& attr : Meta->Attrs) {
            if (!attr.first.StartsWith(YqlViewPrefixAttribute) || attr.first.size() == YqlViewPrefixAttribute.size()) {
                continue;
            }
            views.push_back(attr.first.substr(YqlViewPrefixAttribute.size()));
        }
        std::sort(begin(views), end(views));
        writer.OnBeginList();
        for (const auto& v: views) {
            writer.OnListItem();
            writer.OnStringScalar(v);
        }
        writer.OnEndList();
    }

    writer.OnEndMap();
}

bool TYtTableDescription::Validate(TPosition pos, TStringBuf cluster, TStringBuf tableName, bool withQB,
    const THashMap<std::pair<TString, TString>, TString>& anonymousLabels, TExprContext& ctx) const {
    auto rowSpec = withQB ? QB2RowSpec : RowSpec;
    if (FailOnInvalidSchema
        && !rowSpec
        && !Meta->YqlCompatibleScheme
        && !Meta->InferredScheme
    ) {
        TMaybe<TString> anonLabel;
        for (const auto& x : anonymousLabels) {
            if (x.first.first == cluster && x.second == tableName) {
                anonLabel = x.first.second;
                break;
            }
        }

        if (anonLabel) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Anonymous table '" << *anonLabel << "' must be materialized. Use COMMIT before reading from it."));
        } else if (InferSchemaRows > 0) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Cannot infer schema for table "
                << TString{tableName}.Quote() << ", table is empty").SetCode(TIssuesIds::YT_SCHEMELESS_TABLE, TSeverityIds::S_ERROR));
        } else {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Table "
                << TString{tableName}.Quote()
                << " does not have any scheme attribute supported by YQL,"
                << " you can add \"PRAGMA yt.InferSchema = '1';\" to"
                << " your query in order to use types of first data row"
                << " as scheme.").SetCode(TIssuesIds::YT_SCHEMELESS_TABLE, TSeverityIds::S_ERROR));
        }
        return false;
    }
    return true;
}

void TYtTableDescription::SetConstraintsReady() {
    ConstraintsReady = true;
    if (RowSpec && Constraints) {
        RowSpec->SetConstraints(Constraints);
        if (const auto sorted = Constraints.GetConstraint<TSortedConstraintNode>()) {
            if (const auto distinct = Constraints.GetConstraint<TDistinctConstraintNode>()) {
                RowSpec->UniqueKeys = distinct->IsOrderBy(*sorted);
            }
        }
    }
}

bool TYtTableDescription::FillViews(
    const TString& cluster, const TString& table, TExprContext& ctx,
    IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
    bool allowViewIsolation, IUdfResolver::TPtr udfResolver) {
    return TYtTableDescriptionBase::FillViews(
        TString{YtProviderName}, cluster, table, Meta->Attrs, ctx,
        moduleResolver, urlListerManager, randomProvider, allowViewIsolation, udfResolver);
}

const TYtTableDescription& TYtTablesData::GetTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) const {
    auto p = Tables.FindPtr(std::make_tuple(cluster, table, epoch.GetOrElse(0)));
    YQL_ENSURE(p, "Table description is not defined: " << cluster << '.' << table << "[epoch=" << epoch.GetOrElse(0) << ']');
    return *p;
}

const TYtTableDescription* TYtTablesData::FindTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) const {
    return Tables.FindPtr(std::make_tuple(cluster, table, epoch.GetOrElse(0)));
}

TYtTableDescription& TYtTablesData::GetOrAddTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) {
    return Tables[std::make_tuple(cluster, table, epoch.GetOrElse(0))];
}

TYtTableDescription& TYtTablesData::GetModifTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) {
    auto p = Tables.FindPtr(std::make_tuple(cluster, table, epoch.GetOrElse(0)));
    YQL_ENSURE(p, "Table description is not defined: " << cluster << '.' << table << "[epoch=" << epoch.GetOrElse(0) << ']');
    return *p;
}

TVector<std::pair<TString, TString>> TYtTablesData::GetAllEpochTables(ui32 epoch) const {
    TVector<std::pair<TString, TString>> res;
    res.reserve(Tables.size());
    for (const auto& item: Tables) {
        if (std::get<2>(item.first) == epoch) {
            res.emplace_back(std::get<0>(item.first), std::get<1>(item.first));
        }
    }
    return res;
}

TVector<std::pair<TString, TString>> TYtTablesData::GetAllZeroEpochTables() const {
    return GetAllEpochTables(0U);
}

void TYtTablesData::ForEach(const std::function<void(const TString&, const TString&, ui32, const TYtTableDescription&)>& cb) const {
    for (const auto& item: Tables) {
        cb(std::get<0>(item.first), std::get<1>(item.first), std::get<2>(item.first), item.second);
    }
}

void TYtTablesData::CleanupCompiledSQL() {
    for (auto& item: Tables) {
        item.second.CleanupCompiledSQL();
    }
}

void TYtState::Reset() {
    LoadEpochMetadata.Clear();
    EpochDependencies.clear();
    Configuration->ClearVersions();
    TablesData = MakeIntrusive<TYtTablesData>();
    AnonymousLabels.clear();
    NodeHash.clear();
    Checkpoints.clear();
    WalkFoldersState.clear();
    NextEpochId = 1;
}

void TYtState::EnterEvaluation(ui64 id) {
    bool res = ConfigurationEvalStates_.emplace(id, Configuration->GetState()).second;
    YQL_ENSURE(res, "Duplicate evaluation state " << id);

    res = EpochEvalStates_.emplace(id, NextEpochId).second;
    YQL_ENSURE(res, "Duplicate evaluation state " << id);
}

void TYtState::LeaveEvaluation(ui64 id) {
    {
        auto it = ConfigurationEvalStates_.find(id);
        YQL_ENSURE(it != ConfigurationEvalStates_.end());
        Configuration->RestoreState(std::move(it->second));
        ConfigurationEvalStates_.erase(it);
    }

    {
        auto it = EpochEvalStates_.find(id);
        YQL_ENSURE(it != EpochEvalStates_.end());
        NextEpochId = it->second;
        EpochEvalStates_.erase(it);
    }
}

std::pair<TIntrusivePtr<TYtState>, TStatWriter> CreateYtNativeState(IYtGateway::TPtr gateway, const TString& userName, const TString& sessionId, const TYtGatewayConfig* ytGatewayConfig, TIntrusivePtr<TTypeAnnotationContext> typeCtx) {
    auto ytState = MakeIntrusive<TYtState>();
    ytState->SessionId = sessionId;
    ytState->Gateway = gateway;
    ytState->Types = typeCtx.Get();
    ytState->DqIntegration_ = CreateYtDqIntegration(ytState.Get());

    if (ytGatewayConfig) {
        std::unordered_set<std::string_view> groups;
        if (ytState->Types->Credentials != nullptr) {
            groups.insert(ytState->Types->Credentials->GetGroups().begin(), ytState->Types->Credentials->GetGroups().end());
        }
        auto filter = [userName, ytState, groups = std::move(groups)](const NYql::TAttr& attr) -> bool {
            if (!attr.HasActivation()) {
                return true;
            }
            if (NConfig::Allow(attr.GetActivation(), userName, groups)) {
                with_lock(ytState->StatisticsMutex) {
                    ytState->Statistics[Max<ui32>()].Entries.emplace_back(TStringBuilder() << "Activation:" << attr.GetName(), 0, 0, 0, 0, 1);
                }
                return true;
            }
            return false;
        };

        ytState->Configuration->Init(*ytGatewayConfig, filter, *typeCtx);
    }

    TStatWriter statWriter = [ytState](ui32 publicId, const TVector<TOperationStatistics::TEntry>& stat) {
        with_lock(ytState->StatisticsMutex) {
            for (size_t i = 0; i < stat.size(); ++i) {
                ytState->Statistics[publicId].Entries.push_back(stat[i]);
            }
        }
    };

    return {ytState, statWriter};
}

TDataProviderInitializer GetYtNativeDataProviderInitializer(IYtGateway::TPtr gateway, ui32 planLimits) {
    return [originalGateway = gateway, planLimits] (
        const TString& userName,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& progressWriter,
        const TYqlOperationOptions& operationOptions,
        THiddenQueryAborter hiddenAborter,
        const TQContext& qContext
    ) {
        Y_UNUSED(functionRegistry);
        Y_UNUSED(randomProvider);
        Y_UNUSED(progressWriter);
        Y_UNUSED(operationOptions);
        Y_UNUSED(hiddenAborter);
        auto gateway = originalGateway;
        if (qContext) {
            gateway = WrapYtGatewayWithQContext(originalGateway, qContext,
                typeCtx->RandomProvider, typeCtx->FileStorage);
        }

        TDataProviderInfo info;
        info.SupportsHidden = true;

        const TYtGatewayConfig* ytGatewayConfig = gatewaysConfig ? &gatewaysConfig->GetYt() : nullptr;
        TIntrusivePtr<TYtState> ytState;
        TStatWriter statWriter;
        std::tie(ytState, statWriter) = CreateYtNativeState(gateway, userName, sessionId, ytGatewayConfig, typeCtx);
        ytState->PlanLimits = planLimits;

        info.Names.insert({TString{YtProviderName}});
        info.Source = CreateYtDataSource(ytState);
        info.Sink = CreateYtDataSink(ytState);
        info.SupportFullResultDataSink = true;
        info.OpenSession = [gateway, statWriter](const TString& sessionId, const TString& username,
            const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
            TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<ITimeProvider> timeProvider) {
            gateway->OpenSession(
                IYtGateway::TOpenSessionOptions(sessionId)
                    .UserName(username)
                    .ProgressWriter(progressWriter)
                    .OperationOptions(operationOptions)
                    .RandomProvider(randomProvider)
                    .TimeProvider(timeProvider)
                    .StatWriter(statWriter)
            );
            return NThreading::MakeFuture();
        };

        info.CleanupSessionAsync = [ytState, gateway](const TString& sessionId) {
            return gateway->CleanupSession(IYtGateway::TCleanupSessionOptions(sessionId));
        };

        info.CloseSessionAsync = [ytState, gateway](const TString& sessionId) {
            return gateway->CloseSession(IYtGateway::TCloseSessionOptions(sessionId)).Apply([ytState](const NThreading::TFuture<void>& future) {
                // do manual cleanup; otherwise there may be dead nodes at program termination
                // in setup with several providers
                ytState->TablesData->CleanupCompiledSQL();

                future.TryRethrow();
            });
        };

        info.TokenResolver = [ytState, gateway](const TString& url, const TString& alias) -> TString {
            TString cluster;
            // assume it is not a YT link at all
            if (!gateway->TryParseYtUrl(url, &cluster, nullptr) && !url.StartsWith("yt:") && alias != "yt") {
                return {};
            }

            // todo: get token by cluster name from Auth when it will be implemented
            if (auto token = ytState->Configuration->Auth.Get()) {
                return *token;
            }

            if (cluster) {
                if (auto p = ytState->Configuration->Tokens.FindPtr(cluster)) {
                    return *p;
                }
            }
            return {};
        };

        return info;
    };
}

namespace {

using namespace NNodes;

struct TYtDataSourceFunctions {
    THashSet<TStringBuf> Names;

    TYtDataSourceFunctions() {
        Names.insert(TEpoch::CallableName());
        Names.insert(TYtMeta::CallableName());
        Names.insert(TYtStat::CallableName());
        Names.insert(TYqlRowSpec::CallableName());
        Names.insert(TYtTable::CallableName());
        Names.insert(TYtRow::CallableName());
        Names.insert(TYtRowRange::CallableName());
        Names.insert(TYtKeyExact::CallableName());
        Names.insert(TYtKeyRange::CallableName());
        Names.insert(TYtPath::CallableName());
        Names.insert(TYtSection::CallableName());
        Names.insert(TYtReadTable::CallableName());
        Names.insert(TYtReadTableScheme::CallableName());
        Names.insert(TYtTableContent::CallableName());
        Names.insert(TYtLength::CallableName());
        Names.insert(TYtConfigure::CallableName());
        Names.insert(TYtTablePath::CallableName());
        Names.insert(TYtTableRecord::CallableName());
        Names.insert(TYtTableIndex::CallableName());
        Names.insert(TYtIsKeySwitch::CallableName());
        Names.insert(TYtRowNumber::CallableName());
        Names.insert(TYtStatOutTable::CallableName());
    }
};

struct TYtDataSinkFunctions {
    THashSet<TStringBuf> Names;

    TYtDataSinkFunctions() {
        Names.insert(TYtOutTable::CallableName());
        Names.insert(TYtOutput::CallableName());
        Names.insert(TYtSort::CallableName());
        Names.insert(TYtCopy::CallableName());
        Names.insert(TYtMerge::CallableName());
        Names.insert(TYtMap::CallableName());
        Names.insert(TYtReduce::CallableName());
        Names.insert(TYtMapReduce::CallableName());
        Names.insert(TYtWriteTable::CallableName());
        Names.insert(TYtFill::CallableName());
        Names.insert(TYtTouch::CallableName());
        Names.insert(TYtDropTable::CallableName());
        Names.insert(TCoCommit::CallableName());
        Names.insert(TYtPublish::CallableName());
        Names.insert(TYtEquiJoin::CallableName());
        Names.insert(TYtStatOut::CallableName());
    }
};

}

const THashSet<TStringBuf>& YtDataSourceFunctions() {
    return Default<TYtDataSourceFunctions>().Names;
}

const THashSet<TStringBuf>& YtDataSinkFunctions() {
    return Default<TYtDataSinkFunctions>().Names;
}

bool TYtState::IsHybridEnabled() const {
    return Types->PureResultDataSource == DqProviderName
        && Configuration->HybridDqExecution.Get().GetOrElse(DefaultHybridDqExecution) && Types->HiddenMode == EHiddenMode::Disable;
}

bool TYtState::IsHybridEnabledForCluster(const std::string_view& cluster) const {
    return !OnlyNativeExecution && Configuration->_EnableDq.Get(TString(cluster)).GetOrElse(true);
}

bool TYtState::HybridTakesTooLong() const {
    return TimeSpentInHybrid + (HybridInFlightOprations.empty() ? TDuration::Zero() : NMonotonic::TMonotonic::Now() - HybridStartTime)
            > Configuration->HybridDqTimeSpentLimit.Get().GetOrElse(TDuration::Minutes(20));
}

}
