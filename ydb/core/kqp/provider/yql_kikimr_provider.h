#pragma once

#include "yql_kikimr_gateway.h"
#include "yql_kikimr_settings.h"

#include <ydb/core/external_sources/external_source_factory.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/library/yql/ast/yql_gc_nodes.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/cache/cache.h>

#include <util/generic/flags.h>
#include <util/generic/is_in.h>
#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NGRpcService {

class IRequestCtxMtSafe;

}
}

namespace NYql {

const TStringBuf KikimrMkqlProtoFormat = "mkql_proto";

class IKikimrRemoteGateway;

struct TKikimrQueryDeadlines {
    TInstant CancelAt;
    TInstant TimeoutAt;
};

enum class EKikimrStatsMode {
    None = 0,
    Basic = 1,
    Full = 2,
    Profile = 3,
};

class IKikimrQueryExecutor : public TThrRefBase {
public:
    using TQueryResult = IKikimrGateway::TQueryResult;
    using TAsyncQueryResult = IKikimrAsyncResult<TQueryResult>;

    struct TExecuteSettings {
        bool CommitTx = false;
        bool RollbackTx = false;
        TKikimrQueryDeadlines Deadlines;
        TKikimrQueryLimits Limits;
        bool RawResults = false; // TODO: deprecate
        TMaybe<bool> UseScanQuery;
        EKikimrStatsMode StatsMode = EKikimrStatsMode::None;
        TMaybe<bool> DocumentApiRestricted;
        TMaybe<NKikimrKqp::TRlPath> RlPath;
    };

    virtual ~IKikimrQueryExecutor() {}

    virtual TIntrusivePtr<TAsyncQueryResult> ExecuteDataQuery(const TString& cluster,
        const TExprNode::TPtr& query, TExprContext& ctx, const TExecuteSettings& settings) = 0;

    virtual TIntrusivePtr<TAsyncQueryResult> ExplainDataQuery(const TString& cluster,
        const TExprNode::TPtr& query, TExprContext& ctx) = 0;
};

enum class EKikimrQueryType {
    Unspecified = 0,
    Dml,
    Ddl,
    YqlScript,
    YqlInternal,
    Scan,
    YqlScriptStreaming,
    Query, // ExecuteQuery
    Script, // ExecuteScript
};

struct TKikimrQueryContext : TThrRefBase {
    TKikimrQueryContext(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<ITimeProvider> timeProvider, TIntrusivePtr<IRandomProvider> randomProvider)
    {
        QueryData = std::make_shared<NKikimr::NKqp::TQueryData>(functionRegistry, timeProvider, randomProvider);
    }

    TKikimrQueryContext(const TKikimrQueryContext&) = delete;
    TKikimrQueryContext& operator=(const TKikimrQueryContext&) = delete;

    bool PrepareOnly = false;

    /*
     * Defuse DDL-prohibiting checks when PrepareOnly = true. Used in scripting query explain.
     */
    bool SuppressDdlChecks = false;

    EKikimrStatsMode StatsMode = EKikimrStatsMode::None;
    EKikimrQueryType Type = EKikimrQueryType::Unspecified;
    TKikimrQueryDeadlines Deadlines;
    TKikimrQueryLimits Limits;

    // Operations on document API tables are performed in restricted mode by default,
    // full mode can be enabled explicitly.
    bool DocumentApiRestricted = true;
    bool IsInternalCall = false;
    bool ConcurrentResults = true;

    std::unique_ptr<NKikimrKqp::TPreparedQuery> PreparingQuery;
    std::shared_ptr<const NKikimrKqp::TPreparedQuery> PreparedQuery;
    NKikimr::NKqp::TQueryData::TPtr QueryData;

    THashMap<ui64, IKikimrQueryExecutor::TQueryResult> Results;
    THashMap<ui64, TIntrusivePtr<IKikimrQueryExecutor::TAsyncQueryResult>> InProgress;
    TVector<ui64> ExecutionOrder;

    NActors::TActorId ReplyTarget;
    TMaybe<NKikimrKqp::TRlPath> RlPath;
    // All rpc calls should be made via Session actor.
    // we do not want add extra life time for query context here
    std::shared_ptr<NKikimr::NGRpcService::IRequestCtxMtSafe> RpcCtx;

    void Reset() {
        PrepareOnly = false;
        SuppressDdlChecks = false;
        StatsMode = EKikimrStatsMode::None;
        Type = EKikimrQueryType::Unspecified;
        Deadlines = {};
        Limits = {};

        PreparingQuery.reset();
        PreparedQuery.reset();
        QueryData->Clear();

        Results.clear();
        InProgress.clear();
        ExecutionOrder.clear();

        RlPath.Clear();
        RpcCtx.reset();
    }
};

class TKikimrTableDescription {
public:
    TKikimrTableDescription() {}

    TKikimrTableDescription(const TKikimrTableDescription&) = delete;
    TKikimrTableDescription& operator=(const TKikimrTableDescription&) = delete;

    TKikimrTableMetadataPtr Metadata = nullptr;
    const TStructExprType* SchemeNode = nullptr;
    TMaybe<TString> RelativePath;

    bool Load(TExprContext& ctx, bool withVirtualColumns = false);
    void ToYson(NYson::TYsonWriter& writer) const;

    TMaybe<ui32> GetKeyColumnIndex(const TString& name) const;
    const TTypeAnnotationNode* GetColumnType(const TString& name) const;

    const THashMap<TString, const TTypeAnnotationNode*> GetColumnTypesMap() const { return ColumnTypes; }

    bool DoesExist() const;

    void RequireStats() { NeedsStats = true; }
    bool GetNeedsStats() const { return NeedsStats; }
    ETableType GetTableType() const { return TableType; }
    void SetTableType(ETableType tableType) { TableType = tableType; }

private:
    THashMap<TString, const TTypeAnnotationNode*> ColumnTypes;
    bool NeedsStats = false;
    ETableType TableType;
};

class TKikimrTablesData : public TThrRefBase {
public:
    TKikimrTablesData() {}
    TKikimrTablesData(const TKikimrTablesData&) = delete;
    TKikimrTablesData& operator=(const TKikimrTablesData&) = delete;

    TKikimrTableDescription& GetOrAddTable(const TString& cluster, const TString& database, const TString& table,
        ETableType tableType = ETableType::Table);
    TKikimrTableDescription& GetTable(const TString& cluster, const TString& table);

    const TKikimrTableDescription* EnsureTableExists(const TString& cluster, const TString& table,
        TPositionHandle pos, TExprContext& ctx) const;

    const TKikimrTableDescription& ExistingTable(const TStringBuf& cluster, const TStringBuf& table) const;

    const THashMap<std::pair<TString, TString>, TKikimrTableDescription>& GetTables() const {
        return Tables;
    }

    void Reset() {
        Tables.clear();
    }

private:
    THashMap<std::pair<TString, TString>, TKikimrTableDescription> Tables;
};

enum class TYdbOperation : ui32 {
    CreateTable          = 1 << 0,
    DropTable            = 1 << 1,
    AlterTable           = 1 << 2,
    Select               = 1 << 3,
    Upsert               = 1 << 4,
    Replace              = 1 << 5,
    Update               = 1 << 6,
    Delete               = 1 << 7,
    InsertRevert         = 1 << 8,
    InsertAbort          = 1 << 9,
    ReservedInsertIgnore = 1 << 10,
    UpdateOn             = 1 << 11,
    DeleteOn             = 1 << 12,
    CreateUser           = 1 << 13,
    AlterUser            = 1 << 14,
    DropUser             = 1 << 15,
    CreateGroup          = 1 << 16,
    AlterGroup           = 1 << 17,
    DropGroup            = 1 << 18,
    CreateTopic          = 1 << 19,
    AlterTopic           = 1 << 20,
    DropTopic            = 1 << 21,
    ModifyPermission     = 1 << 22
};

Y_DECLARE_FLAGS(TYdbOperations, TYdbOperation);
Y_DECLARE_OPERATORS_FOR_FLAGS(TYdbOperations);

const TYdbOperations& KikimrSchemeOps();
const TYdbOperations& KikimrDataOps();
const TYdbOperations& KikimrModifyOps();
const TYdbOperations& KikimrReadOps();

TIssue AddDmlIssue(const TIssue& issue);
bool AddDmlIssue(const TIssue& issue, TExprContext& ctx);

class TKikimrTransactionContextBase : public TThrRefBase {
public:
    explicit TKikimrTransactionContextBase(bool enableImmediateEffects) : EnableImmediateEffects(enableImmediateEffects) {
    }

    bool HasStarted() const {
        return EffectiveIsolationLevel.Defined();
    }

    bool IsInvalidated() const {
        return Invalidated;
    }

    bool IsClosed() const {
        return Closed;
    }

    virtual void Finish() {
        Closed = true;
    }

    void Invalidate() {
        if (HasStarted()) {
            Invalidated = true;
        }
    }

    void Reset() {
        TableOperations.clear();
        TableByIdMap.clear();
        EffectiveIsolationLevel.Clear();
        Invalidated = false;
        Readonly = false;
        Closed = false;
        HasUncommittedChangesRead = false;
    }

    template<class IterableKqpTableOps, class IterableKqpTableInfos>
    std::pair<bool, TIssues> ApplyTableOperations(const IterableKqpTableOps& operations,
        const IterableKqpTableInfos& tableInfos, EKikimrQueryType queryType)
    {
        TIssues issues;
        if (IsClosed()) {
            TString message = TStringBuilder() << "Cannot perform operations on closed transaction.";
            issues.AddIssue(YqlIssue({}, TIssuesIds::KIKIMR_BAD_OPERATION, message));
            return {false, issues};
        }

        bool hasScheme = false;
        bool hasData = false;
        for (auto& pair : TableOperations) {
            hasScheme = hasScheme || (pair.second & KikimrSchemeOps());
            hasData = hasData || (pair.second & KikimrDataOps());
        }

        THashMap<TString, NKqpProto::TKqpTableInfo> tableInfoMap;
        for (const auto& info : tableInfos) {
            tableInfoMap.insert(std::make_pair(info.GetTableName(), info));

            TKikimrPathId pathId(info.GetTableId().GetOwnerId(), info.GetTableId().GetTableId());
            TableByIdMap.insert(std::make_pair(pathId, info.GetTableName()));
        }

        for (const auto& op : operations) {
            const auto& table = op.GetTable();

            auto newOp = TYdbOperation(op.GetOperation());
            TPosition pos(op.GetPosition().GetColumn(), op.GetPosition().GetRow());

            const auto info = tableInfoMap.FindPtr(table);
            if (!info) {
                TString message = TStringBuilder()
                    << "Unable to find table info for table '" << table << "'";
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_SCHEME_ERROR, message));
                return {false, issues};
            }

            if (queryType == EKikimrQueryType::Dml && (newOp & KikimrSchemeOps())) {
                TString message = TStringBuilder() << "Operation '" << newOp
                    << "' can't be performed in data query";
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                return {false, issues};
            }

            if (IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, queryType) && (newOp & KikimrSchemeOps())) {
                if (EffectiveIsolationLevel) {
                    TString message = TStringBuilder() << "Scheme operations can't be performed inside transaction, "
                        << "operation: " << newOp;
                    issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                    return {false, issues};
                }
            }

            if (queryType == EKikimrQueryType::Ddl && (newOp & KikimrDataOps())) {
                TString message = TStringBuilder() << "Operation '" << newOp
                    << "' can't be performed in scheme query";
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                return {false, issues};
            }

            if (queryType == EKikimrQueryType::Scan && (newOp & KikimrModifyOps())) {
                TString message = TStringBuilder() << "Operation '" << newOp
                    << "' can't be performed in scan query";
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                return {false, issues};
            }

            if (hasData && (newOp & KikimrSchemeOps()) ||
                hasScheme && (newOp & KikimrDataOps()))
            {
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_MIXED_SCHEME_DATA_TX));
                return {false, issues};
            }

            if (Readonly && (newOp & KikimrModifyOps())) {
                TString message = TStringBuilder() << "Operation '" << newOp
                    << "' can't be performed in read only transaction";
                issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                return {false, issues};
            }

            auto& currentOps = TableOperations[table];
            bool currentModify = currentOps & KikimrModifyOps();
            if (currentModify) {
                if (KikimrReadOps() & newOp) {
                    if (!EnableImmediateEffects) {
                        TString message = TStringBuilder() << "Data modifications previously made to table '" << table
                            << "' in current transaction won't be seen by operation: '"
                            << newOp << "'";
                        auto newIssue = AddDmlIssue(YqlIssue(pos, TIssuesIds::KIKIMR_READ_MODIFIED_TABLE, message));
                        issues.AddIssue(newIssue);
                        return {false, issues};
                    }

                    HasUncommittedChangesRead = true;
                }

                if (info->GetHasIndexTables()) {
                    if (!EnableImmediateEffects) {
                        TString message = TStringBuilder()
                            << "Multiple modification of table with secondary indexes is not supported yet";
                        issues.AddIssue(YqlIssue(pos, TIssuesIds::KIKIMR_BAD_OPERATION, message));
                        return {false, issues};
                    }

                    HasUncommittedChangesRead = true;
                }
            }

            currentOps |= newOp;
        }

        return {true, issues};
    }

    virtual ~TKikimrTransactionContextBase() = default;

public:
    THashMap<TString, TYdbOperations> TableOperations;
    bool HasUncommittedChangesRead = false;
    const bool EnableImmediateEffects;
    THashMap<TKikimrPathId, TString> TableByIdMap;
    TMaybe<NKikimrKqp::EIsolationLevel> EffectiveIsolationLevel;
    bool Readonly = false;
    bool Invalidated = false;
    bool Closed = false;
};

class TKikimrSessionContext : public TThrRefBase {
public:
    TKikimrSessionContext(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TKikimrConfiguration::TPtr config,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<TKikimrTransactionContextBase> txCtx = nullptr)
        : Configuration(config)
        , TablesData(MakeIntrusive<TKikimrTablesData>())
        , QueryCtx(MakeIntrusive<TKikimrQueryContext>(functionRegistry, timeProvider, randomProvider))
        , TxCtx(txCtx) {}

    TKikimrSessionContext(const TKikimrSessionContext&) = delete;
    TKikimrSessionContext& operator=(const TKikimrSessionContext&) = delete;

    TKikimrConfiguration& Config() { return *Configuration; }
    TKikimrTablesData& Tables() { return *TablesData; }
    TKikimrQueryContext& Query() { return *QueryCtx; }
    TKikimrTransactionContextBase& Tx() { Y_VERIFY(HasTx()); return *TxCtx; }

    TKikimrConfiguration::TPtr ConfigPtr() { return Configuration; }
    TIntrusivePtr<TKikimrTablesData> TablesPtr() { return TablesData; }
    TIntrusivePtr<TKikimrQueryContext> QueryPtr() { return QueryCtx; }
    TIntrusivePtr<TKikimrTransactionContextBase> TxPtr() { return TxCtx; }

    bool HasTx() const { return !!TxCtx; }
    void ClearTx() { TxCtx.Reset(); }
    void SetTx(TIntrusivePtr<TKikimrTransactionContextBase>& txCtx) { TxCtx.Reset(txCtx); }

    TString GetUserName() const {
        return UserName;
    }

    void SetUserName(const TString& userName) {
        UserName = userName;
    }

    TString GetDatabase() const {
        return Database;
    }

    void SetDatabase(const TString& database) {
        Database = database;
    }

    void Reset(bool keepConfigChanges) {
        TablesData->Reset();
        QueryCtx->Reset();
        ClearTx();

        if (!keepConfigChanges) {
            Configuration->Restore();
        }
    }

private:
    TString UserName;
    TString Database;
    TKikimrConfiguration::TPtr Configuration;
    TIntrusivePtr<TKikimrTablesData> TablesData;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKikimrTransactionContextBase> TxCtx;
};

TIntrusivePtr<IDataProvider> CreateKikimrDataSource(
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    TTypeAnnotationContext& types,
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& sourceFactory,
    bool isInternalCall);

TIntrusivePtr<IDataProvider> CreateKikimrDataSink(
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    TTypeAnnotationContext& types,
    TIntrusivePtr<IKikimrGateway> gateway,
    TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TIntrusivePtr<IKikimrQueryExecutor> queryExecutor);

} // namespace NYql
