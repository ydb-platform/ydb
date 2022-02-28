#pragma once

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMsgBusProxy {

static const ui32 PQ_METACACHE_TIMEOUT_SECONDS = 120;
static const ui32 PQ_METACACHE_REFRESH_INTERVAL_SECONDS = 10;


inline TActorId CreatePersQueueMetaCacheV2Id() {
    return TActorId(0, "PQMetaCache");
}

namespace NPqMetaCacheV2 {

enum class EQueryType {
    ECheckVersion,
    EGetTopics,
};

struct TTopicMetaRequest {
    TString Path;
    THolder<NSchemeCache::TSchemeCacheNavigate> Response;
    bool Success = false;
};

using TMetaCacheRequest = TVector<TTopicMetaRequest>;

struct TEvPqNewMetaCache {
    enum EEv {
        EvWakeup = EventSpaceBegin(TKikimrEvents::ES_PQ_META_CACHE),
        EvSessionStarted,
        EvQueryPrepared,
        EvQueryComplete,
        EvRestartQuery,
        EvGetVersionRequest,
        EvGetVersionResponse,
        EvDescribeTopicsRequest,
        EvDescribeTopicsResponse,
        EvDescribeAllTopicsRequest,
        EvDescribeAllTopicsResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE)");

    struct TEvSessionStarted : public TEventLocal<TEvSessionStarted, EvSessionStarted> {
    };

    struct TEvQueryPrepared : public TEventLocal<TEvQueryPrepared, EvQueryPrepared> {
    };

    struct TEvQueryComplete : public TEventLocal<TEvQueryComplete, EvQueryComplete> {
        EQueryType Type;

        explicit TEvQueryComplete(EQueryType type)
            : Type(type) {}
    };

    struct TEvRestartQuery : public TEventLocal<TEvRestartQuery, EvRestartQuery> {
        EQueryType Type;

        explicit TEvRestartQuery(EQueryType type)
            : Type(type) {}
    };

    struct TEvGetVersionRequest : public TEventLocal<TEvGetVersionRequest, EvGetVersionRequest> {
    };

    struct TEvGetVersionResponse : public TEventLocal<TEvGetVersionResponse, EvGetVersionResponse> {
        TEvGetVersionResponse(ui64 version)
                : TopicsVersion(version)
        {}

        ui64 TopicsVersion;
    };

    struct TEvDescribeTopicsRequest : public TEventLocal<TEvDescribeTopicsRequest, EvDescribeTopicsRequest> {
        TString PathPrefix;
        TVector<TString> Topics;

        TEvDescribeTopicsRequest() = default;

        explicit TEvDescribeTopicsRequest(const TVector<TString>& topics)
                : Topics(topics)
        {}

        TEvDescribeTopicsRequest(const TVector<TString>& topics, const TString& pathPrefix)
                : PathPrefix(pathPrefix)
                , Topics(topics)
        {}
    };

    struct TEvDescribeTopicsResponse : public TEventLocal<TEvDescribeTopicsResponse, EvDescribeTopicsResponse> {
        TVector<TString> TopicsRequested;
        std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> Result;
        explicit TEvDescribeTopicsResponse(const TVector<TString>& topics,
                                           NSchemeCache::TSchemeCacheNavigate* result)

                : TopicsRequested(topics)
                , Result(result)
        {}
    };

    struct TEvDescribeAllTopicsRequest : public TEventLocal<TEvDescribeAllTopicsRequest, EvDescribeAllTopicsRequest> {
        TString PathPrefix;
        TEvDescribeAllTopicsRequest() = default;
        explicit TEvDescribeAllTopicsRequest(const TString& pathPrefix)
                : PathPrefix(pathPrefix)
        {}
    };

    struct TEvDescribeAllTopicsResponse : public TEventLocal<TEvDescribeAllTopicsResponse, EvDescribeAllTopicsResponse> {
        bool Success = true;
        TString Path;
        std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> Result;
        explicit TEvDescribeAllTopicsResponse(const TString& path)
                : Path(path)
        {}
        TEvDescribeAllTopicsResponse(const TString& path, const std::shared_ptr<NSchemeCache::TSchemeCacheNavigate>& result)
                : Path(path)
                , Result(result)
        {}
    };
};
IActor* CreatePQMetaCache(const NMonitoring::TDynamicCounterPtr& counters,
                          const TDuration& versionCheckInterval);

IActor* CreatePQMetaCache(ui64 grpcPort,
                          const NMonitoring::TDynamicCounterPtr& counters,
                          const TDuration& versionCheckInterval = TDuration::Seconds(1));

} // namespace NPqMetaCacheV2

} //namespace NKikimr::NMsgBusProxy
