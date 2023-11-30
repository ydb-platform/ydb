#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/client/server/msgbus_server_pq_metacache.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <ydb/services/lib/actors/type_definitions.h>


namespace NKikimr::NGRpcProxy::V1 {

class TReadInitAndAuthActor : public NActors::TActorBootstrapped<TReadInitAndAuthActor> {
    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

public:
    TReadInitAndAuthActor(const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
                          const TString& session, const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                          TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
                          const NPersQueue::TTopicsToConverter& topics, const TString& localCluster, bool skipReadRuleCheck = false);

    ~TReadInitAndAuthActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }

private:

    STRICT_STFUNC(StateFunc,
          HFunc(TEvDescribeTopicsResponse, HandleTopicsDescribeResponse)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleClientSchemeCacheResponse)
          HFunc(NActors::TEvents::TEvPoisonPill, HandlePoison)
    );

    void HandlePoison(NActors::TEvents::TEvPoisonPill::TPtr&, const NActors::TActorContext& ctx) {
        Die(ctx);
    }

    void CloseSession(const TString& errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode code, const TActorContext& ctx);

    void DescribeTopics(const NActors::TActorContext& ctx, bool showPrivate = false);
    bool ProcessTopicSchemeCacheResponse(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry,
                                         THashMap<TString, TTopicHolder>::iterator topicsIter, const TActorContext& ctx);
    void HandleClientSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void SendCacheNavigateRequest(const TActorContext& ctx, const TString& path);

    void HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void FinishInitialization(const NActors::TActorContext& ctx);
    bool CheckTopicACL(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry, const TString& topic, const TActorContext& ctx);
    void CheckClientACL(const TActorContext& ctx);

    bool CheckACLPermissionsForNavigate(const TIntrusivePtr<TSecurityObject>& secObject,
                                        const TString& path, NACLib::EAccessRights rights,
                                        const TString& errorTextWhenAccessDenied,
                                        const TActorContext& ctx);

private:
    const TActorId ParentId;
    const ui64 Cookie;
    const TString Session;

    const TActorId MetaCacheId;
    const TActorId NewSchemeCache;

    const TString ClientId;
    const TString ClientPath;
    const bool SkipReadRuleCheck;

    TIntrusiveConstPtr<NACLib::TUserToken> Token;

    THashMap<TString, TTopicHolder> Topics; // topic -> info

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    bool DoCheckACL;

    TString LocalCluster;
};

}
