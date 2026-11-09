#pragma once

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>

#include <library/cpp/containers/absl/flat_hash_set.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/services/lib/actors/type_definitions.h>


namespace NKikimr::NGRpcProxy::V1 {

class TReadInitAndAuthActor : public NPQ::TBaseActor<TReadInitAndAuthActor>
                            , public NPQ::TConstantLogPrefix {
    using TBase = NPQ::TBaseActor<TReadInitAndAuthActor>;
    using TEvDescribeTopicsResponse = NPQ::NDescriber::TEvDescribeTopicsResponse;

public:
    TReadInitAndAuthActor(const TActorContext& ctx, const TActorId& parentId, const TString& clientId, const ui64 cookie,
                          const TString& session, const TString& database, const NActors::TActorId& newSchemeCache,
                          TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, TIntrusiveConstPtr<NACLib::TUserToken> token,
                          const NPersQueue::TTopicsToConverter& topics, const TString& localCluster, bool skipReadRuleCheck = false);

    ~TReadInitAndAuthActor();

    void Bootstrap(const NActors::TActorContext& ctx);
    void Die(const NActors::TActorContext& ctx) override;
    bool OnUnhandledException(const std::exception& exc) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }

    NPQ::TStructuredMessage BuildLogPrefix() const override {
        return YDB_LOG_CREATE_MESSAGE(
            {"sessionCookie", Cookie},
            {"consumer", ClientPath},
            {"session", Session});
    }

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

    void DescribeTopics(const NActors::TActorContext& ctx);
    void DescribePaths(absl::flat_hash_set<TString> topics);
    bool ProcessTopicSchemeCacheResponse(const NPQ::NDescriber::TTopicInfo& topicInfo,
                                         THashMap<TString, TTopicHolder>::iterator topicsIter, const TActorContext& ctx);
    void HandleClientSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
    void SendCacheNavigateRequest(const TActorContext& ctx, const TString& path);

    void HandleTopicsDescribeResponse(TEvDescribeTopicsResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void FinishInitialization(const NActors::TActorContext& ctx);
    bool CheckTopicACL(const NPQ::NDescriber::TTopicInfo& topicInfo, const TString& topic, const TActorContext& ctx);
    void CheckClientACL(const TActorContext& ctx);

    bool CheckACLPermissionsForNavigate(const TIntrusivePtr<TSecurityObject>& secObject,
                                        const TString& path, NACLib::EAccessRights rights,
                                        const TString& errorTextWhenAccessDenied,
                                        const TActorContext& ctx);

private:
    const TActorId ParentId;
    const ui64 Cookie;
    const TString Session;

    const TString Database;
    const TActorId NewSchemeCache;
    TActorId DescriberId;
    bool SecondaryDescribe = false;
    // secondary scheme path -> Topics key (primary path)
    THashMap<TString, TString> SecondaryToOriginal;

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
