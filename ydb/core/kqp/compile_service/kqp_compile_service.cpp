#include "kqp_compile_service.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/cache/cache.h>

#include <util/string/escape.h>

LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

using namespace NKikimrConfig;
using namespace NYql;


class TKqpQueryCache {
public:
    TKqpQueryCache(size_t size, TDuration ttl)
        : List(size)
        , Ttl(ttl) {}

    bool Insert(const TKqpCompileResult::TConstPtr& compileResult) {
        Y_ENSURE(compileResult->Query);
        auto& query = *compileResult->Query;

        auto queryIt = QueryIndex.emplace(query, compileResult->Uid);
        Y_ENSURE(queryIt.second);

        auto it = Index.emplace(compileResult->Uid, TCacheEntry{compileResult,
                                    TAppData::TimeProvider->Now() + Ttl});
        Y_VERIFY(it.second);

        TItem* item = &const_cast<TItem&>(*it.first);
        auto removedItem = List.Insert(item);

        IncBytes(item->Value.CompileResult->PreparedQuery->ByteSize());

        if (removedItem) {
            DecBytes(removedItem->Value.CompileResult->PreparedQuery->ByteSize());

            QueryIndex.erase(*removedItem->Value.CompileResult->Query);
            auto indexIt = Index.find(*removedItem);
            if (indexIt != Index.end()) {
                Index.erase(indexIt);
            }
        }

        Y_VERIFY(List.GetSize() == Index.size());
        Y_VERIFY(List.GetSize() == QueryIndex.size());

        return removedItem != nullptr;
    }

    TKqpCompileResult::TConstPtr FindByUid(const TString& uid, bool promote) {
        auto it = Index.find(TItem(uid));
        if (it != Index.end()) {
            TItem* item = &const_cast<TItem&>(*it);
            if (promote) {
                item->Value.ExpiredAt = TAppData::TimeProvider->Now() + Ttl;
                List.Promote(item);
            }

            return item->Value.CompileResult;
        }

        return nullptr;
    }

    void Replace(const TKqpCompileResult::TConstPtr& compileResult) {
        auto it = Index.find(TItem(compileResult->Uid));
        if (it != Index.end()) {
            TItem& item = const_cast<TItem&>(*it);
            item.Value.CompileResult = compileResult;
        }
    }

    TKqpCompileResult::TConstPtr FindByQuery(const TKqpQueryId& query, bool promote) {
        auto uid = QueryIndex.FindPtr(query);
        if (!uid) {
            return nullptr;
        }

        return FindByUid(*uid, promote);
    }

    bool EraseByUid(const TString& uid) {
        auto it = Index.find(TItem(uid));
        if (it == Index.end()) {
            return false;
        }

        TItem* item = &const_cast<TItem&>(*it);
        List.Erase(item);

        DecBytes(item->Value.CompileResult->PreparedQuery->ByteSize());

        Y_VERIFY(item->Value.CompileResult);
        Y_VERIFY(item->Value.CompileResult->Query);
        QueryIndex.erase(*item->Value.CompileResult->Query);

        Index.erase(it);

        Y_VERIFY(List.GetSize() == Index.size());
        Y_VERIFY(List.GetSize() == QueryIndex.size());
        return true;
    }

    size_t Size() const {
        return Index.size();
    }

    ui64 Bytes() const {
        return ByteSize;
    }

    size_t EraseExpiredQueries() {
        auto prevSize = Size();

        auto now = TAppData::TimeProvider->Now();
        while (List.GetSize() && List.GetOldest()->Value.ExpiredAt <= now) {
            EraseByUid(List.GetOldest()->Key);
        }

        Y_VERIFY(List.GetSize() == Index.size());
        Y_VERIFY(List.GetSize() == QueryIndex.size());
        return prevSize - Size();
    }

    void Clear() {
        List = TList(List.GetMaxSize());
        Index.clear();
        QueryIndex.clear();
        ByteSize = 0;
    }

private:
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
    };

    using TList = TLRUList<TString, TCacheEntry>;
    using TItem = TList::TItem;

private:
    TList List;
    THashSet<TItem, TItem::THash> Index;
    THashMap<TKqpQueryId, TString, THash<TKqpQueryId>> QueryIndex;
    ui64 ByteSize = 0;
    TDuration Ttl;
};

struct TKqpCompileRequest {
    TKqpCompileRequest(const TActorId& sender, const TString& uid, TKqpQueryId query, bool keepInCache,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TInstant& deadline, TKqpDbCountersPtr dbCounters,
        ui64 cookie, NLWTrace::TOrbit orbit = {}, NWilson::TSpan span = {})
        : Sender(sender)
        , Query(std::move(query))
        , Uid(uid)
        , KeepInCache(keepInCache)
        , UserToken(userToken)
        , Deadline(deadline)
        , DbCounters(dbCounters)
        , Orbit(std::move(orbit))
        , CompileServiceSpan(std::move(span))
        , Cookie(cookie)
    {}

    TActorId Sender;
    TKqpQueryId Query;
    TString Uid;
    bool KeepInCache = false;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TInstant Deadline;
    TKqpDbCountersPtr DbCounters;
    TActorId CompileActor;

    NLWTrace::TOrbit Orbit;
    NWilson::TSpan CompileServiceSpan;
    ui64 Cookie;
};

class TKqpRequestsQueue {
    using TRequestsList = TList<TKqpCompileRequest>;
    using TRequestsIterator = TRequestsList::iterator;

    struct TRequestsIteratorHash {
        inline size_t operator()(const TRequestsIterator& iterator) const {
            return THash<TKqpCompileRequest*>()(&*iterator);
        }
    };

    using TRequestsIteratorSet = THashSet<TRequestsIterator, TRequestsIteratorHash>;

public:
    TKqpRequestsQueue(size_t maxSize)
        : MaxSize(maxSize) {}

    bool Enqueue(TKqpCompileRequest&& request) {
        if (Size() >= MaxSize) {
            return false;
        }

        Queue.push_back(std::move(request));
        auto it = std::prev(Queue.end());
        QueryIndex[it->Query].insert(it);
        return true;
    }

    TMaybe<TKqpCompileRequest> Dequeue() {
        for (auto it = Queue.begin(); it != Queue.end(); ++it) {
            auto& request = *it;
            if (!ActiveRequests.contains(request.Query)) {
                auto result = std::move(request);

                QueryIndex[result.Query].erase(it);
                Queue.erase(it);

                return result;
            }
        }

        return {};
    }

    TVector<TKqpCompileRequest> ExtractByQuery(const TKqpQueryId& query) {
        auto queryIt = QueryIndex.find(query);
        if (queryIt == QueryIndex.end()) {
            return {};
        }

        TVector<TKqpCompileRequest> result;
        for (auto& requestIt : queryIt->second) {
            Y_ENSURE(requestIt != Queue.end());
            auto request = std::move(*requestIt);

            Queue.erase(requestIt);

            result.push_back(std::move(request));
        }

        QueryIndex.erase(queryIt);
        return result;
    }

    size_t Size() const {
        return Queue.size();
    }

    TKqpCompileRequest FinishActiveRequest(const TKqpQueryId& query) {
        auto it = ActiveRequests.find(query);
        Y_ENSURE(it != ActiveRequests.end());

        auto request = std::move(it->second);
        ActiveRequests.erase(it);

        return request;
    }

    size_t ActiveRequestsCount() const {
        return ActiveRequests.size();
    }

    void AddActiveRequest(TKqpCompileRequest&& request) {
        auto result = ActiveRequests.emplace(request.Query, std::move(request));
        Y_ENSURE(result.second);
    }

private:
    size_t MaxSize = 0;
    TRequestsList Queue;
    THashMap<TKqpQueryId, TRequestsIteratorSet> QueryIndex;
    THashMap<TKqpQueryId, TKqpCompileRequest> ActiveRequests;
};

class TKqpCompileService : public TActorBootstrapped<TKqpCompileService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_COMPILE_SERVICE;
    }

    TKqpCompileService(const TTableServiceConfig& serviceConfig, const TKqpSettings::TConstPtr& kqpSettings,
        TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
        std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
        NYql::IHTTPGateway::TPtr httpGateway)
        : Config(serviceConfig)
        , KqpSettings(kqpSettings)
        , ModuleResolverState(moduleResolverState)
        , Counters(counters)
        , QueryCache(Config.GetCompileQueryCacheSize(), TDuration::Seconds(Config.GetCompileQueryCacheTTLSec()))
        , RequestsQueue(Config.GetCompileRequestQueueSize())
        , QueryReplayFactory(std::move(queryReplayFactory))
        , HttpGateway(std::move(httpGateway))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);

        QueryReplayBackend.Reset(CreateQueryReplayBackend(Config, Counters, QueryReplayFactory));
        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        Become(&TKqpCompileService::MainState);
        if (Config.GetCompileQueryCacheTTLSec()) {
            StartCheckQueriesTtlTimer();
        }
    }

private:
    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqp::TEvCompileRequest, Handle);
            HFunc(TEvKqp::TEvCompileResponse, Handle);
            HFunc(TEvKqp::TEvCompileInvalidateRequest, Handle);
            HFunc(TEvKqp::TEvRecompileRequest, Handle);

            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
            hFunc(TEvents::TEvUndelivered, HandleUndelivery);

            CFunc(TEvents::TSystem::Wakeup, HandleTtlTimer);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        default:
            Y_FAIL("TKqpCompileService: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_INFO(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, "Subscribed for config changes");
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        bool enableKqpDataQueryStreamLookup = Config.GetEnableKqpDataQueryStreamLookup();
        bool enableKqpScanQueryStreamLookup = Config.GetEnableKqpScanQueryStreamLookup();
        bool enableKqpScanQueryStreamIdxLookupJoin = Config.GetEnableKqpScanQueryStreamIdxLookupJoin();

        bool enableKqpDataQuerySourceRead = Config.GetEnableKqpDataQuerySourceRead();
        bool enableKqpScanQuerySourceRead = Config.GetEnableKqpScanQuerySourceRead();

        bool enableKqpDataQueryPredicateExtract = Config.GetEnablePredicateExtractForDataQueries();
        bool enableKqpScanQueryPredicateExtract = Config.GetEnablePredicateExtractForScanQueries();
        bool predicateExtract20 = Config.GetPredicateExtract20();

        bool enableSequentialReads = Config.GetEnableSequentialReads();
        bool defaultSyntaxVersion = Config.GetSqlVersion();
        bool enableKqpImmediateEffects = Config.GetEnableKqpImmediateEffects();

        Config.Swap(event.MutableConfig()->MutableTableServiceConfig());
        LOG_INFO(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE, "Updated config");

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);

        if (Config.GetSqlVersion() != defaultSyntaxVersion ||
            Config.GetEnableKqpDataQueryStreamLookup() != enableKqpDataQueryStreamLookup ||
            Config.GetEnableKqpScanQueryStreamLookup() != enableKqpScanQueryStreamLookup ||
            Config.GetEnableKqpScanQueryStreamIdxLookupJoin() != enableKqpScanQueryStreamIdxLookupJoin ||
            Config.GetEnableKqpDataQuerySourceRead() != enableKqpDataQuerySourceRead ||
            Config.GetEnableKqpScanQuerySourceRead() != enableKqpScanQuerySourceRead ||
            Config.GetEnablePredicateExtractForDataQueries() != enableKqpDataQueryPredicateExtract ||
            Config.GetEnablePredicateExtractForScanQueries() != enableKqpScanQueryPredicateExtract ||
            Config.GetPredicateExtract20() != predicateExtract20 ||
            Config.GetEnableSequentialReads() != enableSequentialReads ||
            Config.GetEnableKqpImmediateEffects() != enableKqpImmediateEffects) {

            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE,
                "Iterator read flags was changed. StreamLookup from " << enableKqpDataQueryStreamLookup <<
                " to " << Config.GetEnableKqpDataQueryStreamLookup() << " for data queries, from " <<
                enableKqpScanQueryStreamLookup << " to " << Config.GetEnableKqpScanQueryStreamLookup() << ", from "
                << enableKqpScanQueryStreamIdxLookupJoin << " to " << Config.GetEnableKqpScanQueryStreamIdxLookupJoin()
                << " scan queries. Sources for data queries from " << enableKqpDataQuerySourceRead << " to "
                << Config.GetEnableKqpDataQuerySourceRead() << "for scan queries from " << enableKqpScanQuerySourceRead
                << " to " << Config.GetEnableKqpScanQuerySourceRead());

            QueryCache.Clear();
        }
    }

    void HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE,
                    "Failed to deliver subscription request to config dispatcher");
                break;
            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_ERROR(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE,
                    "Failed to deliver config notification response");
                break;
            default:
                LOG_ERROR(*TlsActivationContext, NKikimrServices::KQP_COMPILE_SERVICE,
                    "Undelivered event with unexpected source type: %d", ev->Get()->SourceType);
                break;
        }
    }

    void Handle(TEvKqp::TEvCompileRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& query = ev->Get()->Query;
        LWTRACK(KqpCompileServiceHandleRequest,
            ev->Get()->Orbit,
            query ? query->UserSid : 0);

        try {
            PerformRequest(ev, ctx);
        }
        catch (const std::exception& e) {
            LogException("TEvCompileRequest", ev->Sender, e, ctx);
            ReplyInternalError(ev->Sender, "", e.what(), ctx, ev->Cookie, std::move(ev->Get()->Orbit), {});
        }
    }

    void PerformRequest(TEvKqp::TEvCompileRequest::TPtr& ev, const TActorContext& ctx) {
        auto& request = *ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Perform request, TraceId.SpanIdPtr: " << ev->TraceId.GetSpanIdPtr());

        NWilson::TSpan CompileServiceSpan(TWilsonKqp::CompileService, std::move(ev->TraceId), "CompileService");

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Received compile request"
            << ", sender: " << ev->Sender
            << ", queryUid: " << (request.Uid ? *request.Uid : "<empty>")
            << ", queryText: \"" << (request.Query ? EscapeC(request.Query->Text) : "<empty>") << "\""
            << ", keepInCache: " << request.KeepInCache);

        *Counters->CompileQueryCacheSize = QueryCache.Size();
        *Counters->CompileQueryCacheBytes = QueryCache.Bytes();

        auto userSid = request.UserToken->GetUserSID();
        auto dbCounters = request.DbCounters;

        if (request.Uid) {
            Counters->ReportCompileRequestGet(dbCounters);

            auto compileResult = QueryCache.FindByUid(*request.Uid, request.KeepInCache);
            if (compileResult) {
                Y_ENSURE(compileResult->Query);
                if (compileResult->Query->UserSid == userSid) {
                    Counters->ReportQueryCacheHit(dbCounters, true);

                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Served query from cache by uid"
                        << ", sender: " << ev->Sender
                        << ", queryUid: " << *request.Uid);

                    ReplyFromCache(ev->Sender, compileResult, ctx, ev->Cookie, std::move(ev->Get()->Orbit), std::move(CompileServiceSpan));
                    return;
                } else {
                    LOG_NOTICE_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Non-matching user sid for query"
                        << ", sender: " << ev->Sender
                        << ", queryUid: " << *request.Uid
                        << ", expected sid: " <<  compileResult->Query->UserSid
                        << ", actual sid: " << userSid);
                }
            }

            Counters->ReportQueryCacheHit(dbCounters, false);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Query not found"
                << ", sender: " << ev->Sender
                << ", queryUid: " << *request.Uid);

            NYql::TIssue issue(NYql::TPosition(), TStringBuilder() << "Query not found: " << *request.Uid);
            ReplyError(ev->Sender, *request.Uid, Ydb::StatusIds::NOT_FOUND, {issue}, ctx, ev->Cookie, std::move(ev->Get()->Orbit), std::move(CompileServiceSpan));
            return;
        }

        Counters->ReportCompileRequestCompile(dbCounters);

        Y_ENSURE(request.Query);
        auto& query = *request.Query;

        if (query.UserSid.empty()) {
            query.UserSid = userSid;
        } else {
            Y_ENSURE(query.UserSid == userSid);
        }

        auto compileResult = QueryCache.FindByQuery(query, request.KeepInCache);
        if (compileResult) {
            Counters->ReportQueryCacheHit(dbCounters, true);

            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Served query from cache"
                << ", sender: " << ev->Sender
                << ", queryUid: " << compileResult->Uid);

            ReplyFromCache(ev->Sender, compileResult, ctx, ev->Cookie, std::move(ev->Get()->Orbit), std::move(CompileServiceSpan));
            return;
        }

        Counters->ReportQueryCacheHit(dbCounters, false);

        LWTRACK(KqpCompileServiceEnqueued,
            ev->Get()->Orbit,
            ev->Get()->Query ? ev->Get()->Query->UserSid : 0);


        TKqpCompileRequest compileRequest(ev->Sender, CreateGuidAsString(), std::move(*request.Query),
            request.KeepInCache, request.UserToken, request.Deadline, dbCounters,
            ev->Cookie,
            std::move(ev->Get()->Orbit), std::move(CompileServiceSpan));

        if (!RequestsQueue.Enqueue(std::move(compileRequest))) {
            Counters->ReportCompileRequestRejected(dbCounters);

            LOG_WARN_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Requests queue size limit exceeded"
                << ", sender: " << ev->Sender
                << ", queueSize: " << RequestsQueue.Size());

            NYql::TIssue issue(NYql::TPosition(), TStringBuilder() <<
                "Exceeded maximum number of requests in compile service queue.");
            ReplyError(ev->Sender, "", Ydb::StatusIds::OVERLOADED, {issue}, ctx, compileRequest.Cookie, std::move(compileRequest.Orbit), std::move(compileRequest.CompileServiceSpan));
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Added request to queue"
            << ", sender: " << ev->Sender
            << ", queueSize: " << RequestsQueue.Size());

        ProcessQueue(ctx);
    }

    void Handle(TEvKqp::TEvRecompileRequest::TPtr& ev, const TActorContext& ctx) {
        try {
            PerformRequest(ev, ctx);
        }
        catch (const std::exception& e) {
            LogException("TEvRecompileRequest", ev->Sender, e, ctx);
            ReplyInternalError(ev->Sender, "", e.what(), ctx, ev->Cookie, std::move(ev->Get()->Orbit), {});
        }
    }

    void PerformRequest(TEvKqp::TEvRecompileRequest::TPtr& ev, const TActorContext& ctx) {
        auto& request = *ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Received recompile request"
            << ", sender: " << ev->Sender);

        auto dbCounters = request.DbCounters;
        Counters->ReportRecompileRequestGet(dbCounters);

        auto compileResult = QueryCache.FindByUid(request.Uid, false);
        if (compileResult || request.Query) {
            Counters->ReportCompileRequestCompile(dbCounters);

            NWilson::TSpan CompileServiceSpan(TWilsonKqp::CompileService, ev->Get() ? std::move(ev->TraceId) : NWilson::TTraceId(), "CompileService");

            TKqpCompileRequest compileRequest(ev->Sender, request.Uid, compileResult ? *compileResult->Query : *request.Query,
                true, request.UserToken, request.Deadline, dbCounters,
                ev->Cookie,
                ev->Get() ? std::move(ev->Get()->Orbit) : NLWTrace::TOrbit(),
                std::move(CompileServiceSpan));

            if (!RequestsQueue.Enqueue(std::move(compileRequest))) {
                Counters->ReportCompileRequestRejected(dbCounters);

                LOG_WARN_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Requests queue size limit exceeded"
                    << ", sender: " << ev->Sender
                    << ", queueSize: " << RequestsQueue.Size());

                NYql::TIssue issue(NYql::TPosition(), TStringBuilder() <<
                    "Exceeded maximum number of requests in compile service queue.");
                ReplyError(ev->Sender, "", Ydb::StatusIds::OVERLOADED, {issue}, ctx, compileRequest.Cookie, std::move(compileRequest.Orbit), std::move(compileRequest.CompileServiceSpan));
                return;
            }
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Query not found"
                << ", sender: " << ev->Sender
                << ", queryUid: " << request.Uid);

            NYql::TIssue issue(NYql::TPosition(), TStringBuilder() << "Query not found: " << request.Uid);

            NWilson::TSpan CompileServiceSpan(TWilsonKqp::CompileService, ev->Get() ? std::move(ev->TraceId) : NWilson::TTraceId(), "CompileService");

            ReplyError(ev->Sender, request.Uid, Ydb::StatusIds::NOT_FOUND, {issue}, ctx,
                ev->Cookie, std::move(ev->Get()->Orbit), std::move(CompileServiceSpan));
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Added request to queue"
            << ", sender: " << ev->Sender
            << ", queueSize: " << RequestsQueue.Size());

        ProcessQueue(ctx);
    }

    void Handle(TEvKqp::TEvCompileResponse::TPtr& ev, const TActorContext& ctx) {
        auto compileActorId = ev->Sender;
        auto& compileResult = ev->Get()->CompileResult;
        auto& compileStats = ev->Get()->Stats;

        Y_VERIFY(compileResult->Query);

        auto compileRequest = RequestsQueue.FinishActiveRequest(*compileResult->Query);
        Y_VERIFY(compileRequest.CompileActor == compileActorId);
        Y_VERIFY(compileRequest.Uid == compileResult->Uid);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Received response"
            << ", sender: " << compileRequest.Sender
            << ", status: " << compileResult->Status
            << ", compileActor: " << ev->Sender);

        try {
            if (compileResult->Status == Ydb::StatusIds::SUCCESS) {
                if (QueryCache.FindByUid(compileResult->Uid, false)) {
                    QueryCache.Replace(compileResult);
                } else if (compileRequest.KeepInCache) {
                    if (QueryCache.Insert(compileResult)) {
                        Counters->CompileQueryCacheEvicted->Inc();
                    }
                }

                if (ev->Get()->ReplayMessage) {
                    QueryReplayBackend->Collect(*ev->Get()->ReplayMessage);
                }

                auto requests = RequestsQueue.ExtractByQuery(*compileResult->Query);
                for (auto& request : requests) {
                    LWTRACK(KqpCompileServiceGetCompilation, request.Orbit, request.Query.UserSid, compileActorId.ToString());
                    Reply(request.Sender, compileResult, compileStats, ctx,
                        request.Cookie, std::move(request.Orbit), std::move(request.CompileServiceSpan));
                }
            } else {
                if (QueryCache.FindByUid(compileResult->Uid, false)) {
                    QueryCache.EraseByUid(compileResult->Uid);
                }
            }

            LWTRACK(KqpCompileServiceGetCompilation, compileRequest.Orbit, compileRequest.Query.UserSid, compileActorId.ToString());
            Reply(compileRequest.Sender, compileResult, compileStats, ctx,
                compileRequest.Cookie, std::move(compileRequest.Orbit), std::move(compileRequest.CompileServiceSpan));
        }
        catch (const std::exception& e) {
            LogException("TEvCompileResponse", ev->Sender, e, ctx);
            ReplyInternalError(compileRequest.Sender, compileResult->Uid, e.what(), ctx,
                compileRequest.Cookie, std::move(compileRequest.Orbit), std::move(compileRequest.CompileServiceSpan));
        }

        ProcessQueue(ctx);
    }

    void Handle(TEvKqp::TEvCompileInvalidateRequest::TPtr& ev, const TActorContext& ctx) {
        try {
            PerformRequest(ev, ctx);
        }
        catch (const std::exception& e) {
            LogException("TEvCompileInvalidateRequest", ev->Sender, e, ctx);
        }
    }

    void PerformRequest(TEvKqp::TEvCompileInvalidateRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto& request = *ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Received invalidate request"
            << ", sender: " << ev->Sender
            << ", queryUid: " << request.Uid);

        auto dbCounters = request.DbCounters;
        Counters->ReportCompileRequestInvalidate(dbCounters);

        QueryCache.EraseByUid(request.Uid);
    }

    void HandleTtlTimer(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Received check queries TTL timeout");

        auto evicted = QueryCache.EraseExpiredQueries();
        if (evicted != 0) {
            Counters->CompileQueryCacheEvicted->Add(evicted);
        }

        StartCheckQueriesTtlTimer();
    }

private:
    void ProcessQueue(const TActorContext& ctx) {
        auto maxActiveRequests = Config.GetCompileMaxActiveRequests();

        while (RequestsQueue.ActiveRequestsCount() < maxActiveRequests) {
            auto request = RequestsQueue.Dequeue();
            if (!request) {
                break;
            }

            if (request->Deadline && request->Deadline < TAppData::TimeProvider->Now()) {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Compilation timed out"
                    << ", sender: " << request->Sender
                    << ", deadline: " << request->Deadline);

                Counters->ReportCompileRequestTimeout(request->DbCounters);

                NYql::TIssue issue(NYql::TPosition(), "Compilation timed out.");
                ReplyError(request->Sender, "", Ydb::StatusIds::TIMEOUT, {issue}, ctx,
                    request->Cookie, std::move(request->Orbit), std::move(request->CompileServiceSpan));
            } else {
                StartCompilation(std::move(*request), ctx);
            }
        }

        *Counters->CompileQueueSize = RequestsQueue.Size();
    }

    void StartCompilation(TKqpCompileRequest&& request, const TActorContext& ctx) {
        auto compileActor = CreateKqpCompileActor(ctx.SelfID, KqpSettings, Config, HttpGateway, ModuleResolverState, Counters,
            request.Uid, request.Query, request.UserToken, request.DbCounters, request.CompileServiceSpan.GetTraceId());
        auto compileActorId = ctx.ExecutorThread.RegisterActor(compileActor, TMailboxType::HTSwap,
            AppData(ctx)->UserPoolId);

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Created compile actor"
            << ", sender: " << request.Sender
            << ", compileActor: " << compileActorId);

        request.CompileActor = compileActorId;
        RequestsQueue.AddActiveRequest(std::move(request));
    }

    void StartCheckQueriesTtlTimer() {
        Schedule(TDuration::Seconds(Config.GetCompileQueryCacheTTLSec()), new TEvents::TEvWakeup());
    }

    void Reply(const TActorId& sender, const TKqpCompileResult::TConstPtr& compileResult,
        const NKqpProto::TKqpStatsCompile& compileStats, const TActorContext& ctx, ui64 cookie,
        NLWTrace::TOrbit orbit, NWilson::TSpan span)
    {
        const auto& query = compileResult->Query;
        LWTRACK(KqpCompileServiceReply,
            orbit,
            query ? query->UserSid : 0,
            compileResult->Issues.ToString());

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Send response"
            << ", sender: " << sender
            << ", queryUid: " << compileResult->Uid
            << ", status:" << compileResult->Status);

        auto responseEv = MakeHolder<TEvKqp::TEvCompileResponse>(compileResult, std::move(orbit));
        responseEv->Stats.CopyFrom(compileStats);

        if (span) {
            span.End();
        }

        ctx.Send(sender, responseEv.Release(), 0, cookie);
    }

    void ReplyFromCache(const TActorId& sender, const TKqpCompileResult::TConstPtr& compileResult,
        const TActorContext& ctx, ui64 cookie, NLWTrace::TOrbit orbit, NWilson::TSpan span)
    {
        NKqpProto::TKqpStatsCompile stats;
        stats.SetFromCache(true);

        LWTRACK(KqpCompileServiceReplyFromCache, orbit);
        Reply(sender, compileResult, stats, ctx, cookie, std::move(orbit), std::move(span));
    }

    void ReplyError(const TActorId& sender, const TString& uid, Ydb::StatusIds::StatusCode status,
        const TIssues& issues, const TActorContext& ctx, ui64 cookie, NLWTrace::TOrbit orbit, NWilson::TSpan span)
    {
        LWTRACK(KqpCompileServiceReplyError, orbit);
        Reply(sender, TKqpCompileResult::Make(uid, status, issues, ETableReadType::Other), NKqpProto::TKqpStatsCompile(), ctx, cookie, std::move(orbit), std::move(span));
    }

    void ReplyInternalError(const TActorId& sender, const TString& uid, const TString& message,
        const TActorContext& ctx, ui64 cookie, NLWTrace::TOrbit orbit, NWilson::TSpan span)
    {
        NYql::TIssue issue(NYql::TPosition(), TStringBuilder() << "Internal error during query compilation.");
        issue.AddSubIssue(MakeIntrusive<TIssue>(NYql::TPosition(), message));

        LWTRACK(KqpCompileServiceReplyInternalError, orbit);
        ReplyError(sender, uid, Ydb::StatusIds::INTERNAL_ERROR, {issue}, ctx, cookie, std::move(orbit), std::move(span));
    }

    static void LogException(const TString& scope, const TActorId& sender, const std::exception& e,
        const TActorContext& ctx)
    {
        LOG_CRIT_S(ctx, NKikimrServices::KQP_COMPILE_SERVICE, "Exception"
            << ", scope: " << scope
            << ", sender: " << sender
            << ", message: " << e.what());
    }

private:
    TTableServiceConfig Config;
    TKqpSettings::TConstPtr KqpSettings;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TIntrusivePtr<TKqpCounters> Counters;
    THolder<IQueryReplayBackend> QueryReplayBackend;

    TKqpQueryCache QueryCache;
    TKqpRequestsQueue RequestsQueue;
    std::shared_ptr<IQueryReplayBackendFactory> QueryReplayFactory;
    NYql::IHTTPGateway::TPtr HttpGateway;
};

IActor* CreateKqpCompileService(const TTableServiceConfig& serviceConfig, const TKqpSettings::TConstPtr& kqpSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    std::shared_ptr<IQueryReplayBackendFactory> queryReplayFactory,
    NYql::IHTTPGateway::TPtr httpGateway)
{
    return new TKqpCompileService(serviceConfig, kqpSettings, moduleResolverState, counters,
            std::move(queryReplayFactory), std::move(httpGateway));
}

} // namespace NKqp
} // namespace NKikimr
