#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
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
        EvGetVersionRequest,
        EvGetVersionResponse,
        EvDescribeTopicsRequest,
        EvDescribeTopicsResponse,
        EvDescribeAllTopicsRequest,
        EvDescribeAllTopicsResponse,
        EvGetNodesMappingRequest,
        EvGetNodesMappingResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE)");


    struct TEvGetVersionRequest : public TEventLocal<TEvGetVersionRequest, EvGetVersionRequest> {
    };

    struct TEvGetVersionResponse : public TEventLocal<TEvGetVersionResponse, EvGetVersionResponse> {
        TEvGetVersionResponse(ui64 version)
                : TopicsVersion(version)
        {}

        ui64 TopicsVersion;
    };

    struct TEvDescribeTopicsRequest : public TEventLocal<TEvDescribeTopicsRequest, EvDescribeTopicsRequest> {
        TVector<NPersQueue::TDiscoveryConverterPtr> Topics;
        bool SyncVersion;
        bool ShowPrivate = false;

        TEvDescribeTopicsRequest() = default;


        explicit TEvDescribeTopicsRequest(const TVector<NPersQueue::TDiscoveryConverterPtr>& topics,
                                          bool syncVersion = true, bool showPrivate = false)
            : Topics(topics)
                , SyncVersion(syncVersion)
                , ShowPrivate(showPrivate)
        {}
    };

    struct TEvDescribeTopicsResponse : public TEventLocal<TEvDescribeTopicsResponse, EvDescribeTopicsResponse> {
        TVector<NPersQueue::TDiscoveryConverterPtr> TopicsRequested;
        std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> Result;
        explicit TEvDescribeTopicsResponse(TVector<NPersQueue::TDiscoveryConverterPtr>&& topics,
                                           const std::shared_ptr<NSchemeCache::TSchemeCacheNavigate>& result)

            : TopicsRequested(std::move(topics))
            , Result(result)
        {}
    };

    struct TEvDescribeAllTopicsRequest : public TEventLocal<TEvDescribeAllTopicsRequest, EvDescribeAllTopicsRequest> {
        TEvDescribeAllTopicsRequest() = default;
    };

    struct TEvDescribeAllTopicsResponse : public TEventLocal<TEvDescribeAllTopicsResponse, EvDescribeAllTopicsResponse> {
        bool Success = true;
        TString Path;
        TVector<NPersQueue::TTopicConverterPtr> Topics;
        std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> Result;

        explicit TEvDescribeAllTopicsResponse() {}

        TEvDescribeAllTopicsResponse(const TString& path, TVector<NPersQueue::TTopicConverterPtr>&& topics,
                                     const std::shared_ptr<NSchemeCache::TSchemeCacheNavigate>& result)
            : Path(path)
            , Topics(std::move(topics))
            , Result(result)
        {}
    };

    struct TEvGetNodesMappingRequest : public TEventLocal<TEvGetNodesMappingRequest, EvGetNodesMappingRequest> {
    };

    struct TEvGetNodesMappingResponse : public TEventLocal<TEvGetNodesMappingResponse, EvGetNodesMappingResponse> {
        std::shared_ptr<THashMap<ui32, ui32>> NodesMapping;
        bool Status;

        TEvGetNodesMappingResponse(const std::shared_ptr<THashMap<ui32, ui32>>& nodesMapping, bool status)
            : NodesMapping(std::move(nodesMapping))
            , Status(status)
        {}

    };

};
IActor* CreatePQMetaCache(const ::NMonitoring::TDynamicCounterPtr& counters,
                          const TDuration& versionCheckInterval = TDuration::Seconds(1));

IActor* CreatePQMetaCache(const NActors::TActorId& schemeBoardCacheId,
                          const TDuration& versionCheckInterval = TDuration::Seconds(1));


} // namespace NPqMetaCacheV2

} //namespace NKikimr::NMsgBusProxy
