#pragma once

#include "yql_yt_gateway.h"
#include "yql_yt_table_desc.h"
#include "yql_yt_table.h"
#include "yql_yt_io_discovery_walk_folders.h"

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/ast/yql_constraint.h>

#include <library/cpp/time_provider/monotonic.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/string.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/system/mutex.h>
#include <util/str_stl.h>

#include <utility>
#include <tuple>
#include <unordered_map>

namespace NYql {

struct TYtTableDescription: public TYtTableDescriptionBase {
    TYtTableStatInfo::TPtr Stat;
    TYtTableMetaInfo::TPtr Meta;
    TYqlRowSpecInfo::TPtr RowSpec;
    TYqlRowSpecInfo::TPtr QB2RowSpec;
    TConstraintSet Constraints;
    bool ConstraintsReady = false;
    bool IsAnonymous = false;
    bool IsReplaced = false;
    TMaybe<bool> MonotonicKeys;
    size_t WriteValidateCount = 0;
    TMaybe<TString> Hash;
    TString ColumnGroupSpec;

    bool Fill(
        const TString& cluster, const TString& table, TExprContext& ctx,
        IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
        bool allowViewIsolation, IUdfResolver::TPtr udfResolver);
    void ToYson(NYson::TYsonWriter& writer, const TString& cluster, const TString& table, const TString& view) const;
    bool Validate(TPosition pos, TStringBuf cluster, TStringBuf tableName, bool withQB,
        const THashMap<std::pair<TString, TString>, TString>& anonymousLabels, TExprContext& ctx) const;
    void SetConstraintsReady();
    bool FillViews(
        const TString& cluster, const TString& table, TExprContext& ctx,
        IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
        bool allowViewIsolation, IUdfResolver::TPtr udfResolver);
};

// Anonymous tables are kept by labels
class TYtTablesData: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TYtTablesData>;

    const TYtTableDescription& GetTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) const;
    const TYtTableDescription* FindTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch) const;
    TYtTableDescription& GetOrAddTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch);
    TYtTableDescription& GetModifTable(const TString& cluster, const TString& table, TMaybe<ui32> epoch);
    TVector<std::pair<TString, TString>> GetAllEpochTables(ui32 epoch) const;
    TVector<std::pair<TString, TString>> GetAllZeroEpochTables() const;
    void CleanupCompiledSQL();
    void ForEach(const std::function<void(const TString&, const TString&, ui32, const TYtTableDescription&)>& cb) const;
private:
    using TTableKey = std::tuple<TString, TString, ui32>; // cluster + table + epoch
    THashMap<TTableKey, TYtTableDescription> Tables;
};


struct TYtState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtState>;

    void Reset();
    void EnterEvaluation(ui64 id);
    void LeaveEvaluation(ui64 id);
    bool IsHybridEnabled() const;
    bool IsHybridEnabledForCluster(const std::string_view& cluster) const;
    bool HybridTakesTooLong() const;

    TString SessionId;
    IYtGateway::TPtr Gateway;
    TTypeAnnotationContext* Types = nullptr;
    TMaybe<std::pair<ui32, size_t>> LoadEpochMetadata; // Epoch being committed, settings versions
    THashMap<ui32, TSet<std::pair<TString, TString>>> EpochDependencies; // List of tables, which have to be updated after committing specific epoch
    TYtVersionedConfiguration::TPtr Configuration = MakeIntrusive<TYtVersionedConfiguration>();
    TYtTablesData::TPtr TablesData = MakeIntrusive<TYtTablesData>();
    THashMap<std::pair<TString, TString>, TString> AnonymousLabels; // cluster + label -> name
    std::unordered_map<ui64, TString> NodeHash; // unique id -> hash
    THashMap<ui32, TOperationStatistics> Statistics; // public id -> stat
    THashMap<TString, TOperationStatistics> HybridStatistics; // subfolder -> stat
    THashMap<TString, THashMap<TString, TOperationStatistics>> HybridOpStatistics; // operation name -> subfolder -> stat
    TMutex StatisticsMutex;
    THashSet<std::pair<TString, TString>> Checkpoints; // Set of checkpoint tables
    THolder<IDqIntegration> DqIntegration_;
    ui32 NextEpochId = 1;
    bool OnlyNativeExecution = false;
    bool PassiveExecution = false;
    TDuration TimeSpentInHybrid;
    NMonotonic::TMonotonic HybridStartTime;
    std::unordered_set<ui32> HybridInFlightOprations;
    THashMap<ui64, TWalkFoldersImpl> WalkFoldersState;
    ui32 PlanLimits = 10;

private:
    std::unordered_map<ui64, TYtVersionedConfiguration::TState> ConfigurationEvalStates_;
    std::unordered_map<ui64, ui32> EpochEvalStates_;
};


class TYtGatewayConfig;
std::pair<TIntrusivePtr<TYtState>, TStatWriter> CreateYtNativeState(IYtGateway::TPtr gateway, const TString& userName, const TString& sessionId, const TYtGatewayConfig* ytGatewayConfig, TIntrusivePtr<TTypeAnnotationContext> typeCtx);
TIntrusivePtr<IDataProvider> CreateYtDataSource(TYtState::TPtr state);
TIntrusivePtr<IDataProvider> CreateYtDataSink(TYtState::TPtr state);

TDataProviderInitializer GetYtNativeDataProviderInitializer(IYtGateway::TPtr gateway, ui32 planLimits = 10);

const THashSet<TStringBuf>& YtDataSourceFunctions();
const THashSet<TStringBuf>& YtDataSinkFunctions();

} // NYql
