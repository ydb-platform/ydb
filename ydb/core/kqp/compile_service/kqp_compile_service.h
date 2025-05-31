#pragma once

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/host/kqp_translate.h>

#include <util/system/spinlock.h>

namespace NKikimr {
namespace NKqp {

class TKqpDbCounters;

enum class ECompileActorAction {
    COMPILE,
    PARSE,
    SPLIT,
};

// Cache is shared between query sessions, compile service and kqp proxy.
// Currently we don't use RW lock, because of cache promotions during lookup:
// in benchmark both versions (RW spinlock and adaptive W spinlock) show same
// performance. Might consider RW lock again in future.
class TKqpQueryCache: public TThrRefBase {
public:
    TKqpQueryCache(size_t size, TDuration ttl)
        : List(size)
        , Ttl(ttl) {}

    bool Insert(const TKqpCompileResult::TConstPtr& compileResult, bool isEnableAstCache, bool isPerStatementExecution);
    void AttachReplayMessage(const TString uid, TString replayMessage);

    TString ReplayMessageByUid(const TString uid, TDuration timeout);

    TKqpCompileResult::TConstPtr FindByUid(const TString& uid, bool promote) {
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByUidImpl(uid, promote);
    }

    void Replace(const TKqpCompileResult::TConstPtr& compileResult);

    TKqpCompileResult::TConstPtr FindByQuery(const TKqpQueryId& query, bool promote) {
        TGuard<TAdaptiveLock> guard(Lock);
        return FindByQueryImpl(query, promote);
    }

    // find by either uid or query
    TKqpCompileResult::TConstPtr Find(
        const TMaybe<TString>& uid,
        TMaybe<TKqpQueryId>& query,
        TKqpTempTablesState::TConstPtr tempTablesState,
        bool promote,
        const NACLib::TSID& userSid,
        TIntrusivePtr<TKqpCounters> counters,
        TIntrusivePtr<TKqpDbCounters>& dbCounters,
        const TActorId& sender,
        const TActorContext& ctx);

    TKqpCompileResult::TConstPtr FindByAst(const TKqpQueryId& query, const NYql::TAstParseResult& ast, bool promote);

    bool EraseByUid(const TString& uid) {
        TGuard<TAdaptiveLock> guard(Lock);
        return EraseByUidImpl(uid);
    }

    size_t Size() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return SizeImpl();
    }

    ui64 Bytes() const {
        TGuard<TAdaptiveLock> guard(Lock);
        return BytesImpl();
    }

    ui64 BytesImpl() const {
        return ByteSize;
    }

    size_t EraseExpiredQueries();

    void Clear();

private:
    TKqpCompileResult::TConstPtr FindByUidImpl(const TString& uid, bool promote);
    TKqpCompileResult::TConstPtr FindByQueryImpl(const TKqpQueryId& query, bool promote);
    bool EraseByUidImpl(const TString& uid);

    size_t SizeImpl() const {
        return Index.size();
    }

    void InsertQuery(const TKqpCompileResult::TConstPtr& compileResult);

    void InsertAst(const TKqpCompileResult::TConstPtr& compileResult) {
        Y_ENSURE(compileResult->Query);
        Y_ENSURE(compileResult->GetAst());

        AstIndex.emplace(GetQueryIdWithAst(*compileResult->Query, *compileResult->GetAst()), compileResult->Uid);
    }

    TKqpQueryId GetQueryIdWithAst(const TKqpQueryId& query, const NYql::TAstParseResult& ast);

    void DecBytes(ui64 bytes) {
        if (bytes > ByteSize) {
            ByteSize = 0;
        } else {
            ByteSize -= bytes;
        }
    }

    void IncBytes(ui64 bytes) {
        ByteSize += bytes;
    }

private:
    struct TCacheEntry {
        TKqpCompileResult::TConstPtr CompileResult;
        TInstant ExpiredAt;
        TString ReplayMessage = "";
        TInstant LastReplayTime = TInstant::Zero();
    };

    using TList = TLRUList<TString, TCacheEntry>;
    using TItem = TList::TItem;

private:
    TList List;
    THashSet<TItem, TItem::THash> Index;
    THashMap<TKqpQueryId, TString, THash<TKqpQueryId>> QueryIndex;
    THashMap<TKqpQueryId, TString, THash<TKqpQueryId>> AstIndex;
    ui64 ByteSize = 0;
    TDuration Ttl;

    TAdaptiveLock Lock;
};

using TKqpQueryCachePtr = TIntrusivePtr<TKqpQueryCache>;

bool HasTempTablesNameClashes(
    TKqpCompileResult::TConstPtr compileResult,
    TKqpTempTablesState::TConstPtr tempTablesState,
    bool withSessionId = false);

IActor* CreateKqpCompileService(
    TKqpQueryCachePtr queryCache,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const TKqpSettings::TConstPtr& kqpSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
    TIntrusivePtr<TKqpCounters> counters, std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup
    );

IActor* CreateKqpCompileComputationPatternService(const NKikimrConfig::TTableServiceConfig& serviceConfig,
    TIntrusivePtr<TKqpCounters> counters);

IActor* CreateKqpCompileActor(const TActorId& owner, const TKqpSettings::TConstPtr& kqpSettings,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TString& uid, const TKqpQueryId& query,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TString& clientAddress,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    TKqpDbCountersPtr dbCounters, const TGUCSettings::TPtr& gUCSettings, const TMaybe<TString>& applicationName,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, NWilson::TTraceId traceId = {},
    TKqpTempTablesState::TConstPtr tempTablesState = nullptr,
    ECompileActorAction compileAction = ECompileActorAction::COMPILE,
    TMaybe<TQueryAst> queryAst = {},
    bool collectFullDiagnostics = false,
    bool PerStatementResult = false,
    std::shared_ptr<NYql::TExprContext> ctx = nullptr,
    NYql::TExprNode::TPtr expr = nullptr);

IActor* CreateKqpCompileRequestActor(const TActorId& owner, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TMaybe<TString>& uid,
    TMaybe<TKqpQueryId>&& query, bool keepInCache, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
    ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TTraceId = {});

} // namespace NKqp
} // namespace NKikimr
