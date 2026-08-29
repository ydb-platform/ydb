#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>

#include <ydb/library/aclib/aclib.h>

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <ydb/services/lib/actors/type_definitions.h>


namespace NKikimr::NGRpcProxy::V1 {

class TReadInitAndAuthActor : public NActors::TActorBootstrapped<TReadInitAndAuthActor>
                            , public NActors::IActorExceptionHandler {
public:
    TReadInitAndAuthActor(const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
                          const TString& session, const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                          TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
                          const THashSet<TString>& topicPaths, const TString& database, const TString& localCluster,
                          bool skipReadRuleCheck = false);

    ~TReadInitAndAuthActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;
    bool OnUnhandledException(const std::exception& exc) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }

private:

    STRICT_STFUNC(StateFunc,
          hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, HandleTopicsDescribeResponse)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleClientSchemeCacheResponse)
          HFunc(NActors::TEvents::TEvPoisonPill, HandlePoison)
    );

    void HandlePoison(NActors::TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
        Die(ctx);
    }

    void CloseSession(const TString& errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode code, const TActorContext& ctx);

    void DescribeTopics(const NActors::TActorContext& ctx);
    void HandleClientSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void SendCacheNavigateRequest(const TActorContext& ctx, const TString& path);

    void HandleTopicsDescribeResponse(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev);
    void FinishInitialization(const NActors::TActorContext& ctx);
    bool ProcessTopicInfo(const TString& path, const NPQ::NDescriber::TTopicInfo& info, const TActorContext& ctx);
    bool CheckTopicACL(const NPQ::NDescriber::TTopicInfo& info, const TString& topic, const TActorContext& ctx);
    void CheckClientACL(const TActorContext& ctx);

    bool CheckACLPermissionsForNavigate(const TIntrusivePtr<TSecurityObject>& secObject,
                                        const TString& path, NACLib::EAccessRights rights,
                                        const TString& errorTextWhenAccessDenied,
                                        const TActorContext& ctx);

private:
    const TActorId ParentId;
    const ui64 Cookie;
    const TString Session;

    const TActorId NewSchemeCache;
    const TString Database;

    const TString ClientId;
    const TString ClientPath;
    const bool SkipReadRuleCheck;

    TIntrusiveConstPtr<NACLib::TUserToken> Token;

    THashSet<TString> TopicPaths;
    THashMap<TString, TTopicHolder> Topics; // topic path -> info

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    bool DoCheckACL;

    TString LocalCluster;
};

}
