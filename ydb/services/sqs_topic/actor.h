#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKikimr::NSqsTopic::V1 {

    // Database user-attribute keys that carry the rate-limiter (RU billing)
    // coordination node and topic resource paths. Kept in sync with the gRPC
    // request check actor (ruRlTopicConfig).
    inline constexpr TStringBuf RL_COORDINATION_NODE_ATTR = "serverless_rt_coordination_node_path";
    inline constexpr TStringBuf RL_TOPIC_RESOURCE_ATTR = "serverless_rt_topic_resource_ru";

    template<class TDerived, class TRequest>
    class TGrpcActorBase: public NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest> {
        public:
        using TBase = NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest>;
        using TBase::TBase;


        void ReplyWithError(Ydb::StatusIds::StatusCode status, size_t additionalStatus, const TString& messageText) = delete;

        void ReplyWithError(const NSQS::TError& error) {
            if (TBase::IsDead) {
                return;
            }

            NYql::TIssue issue(error.GetMessage());
            issue.SetCode(
                NSQS::TErrorClass::GetId(error.GetErrorCode()),
                NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
            this->Request_->RaiseIssue(issue);
            this->Request_->ReplyWithYdbStatus(Ydb::StatusIds_StatusCode_STATUS_CODE_UNSPECIFIED);
            this->Die(this->ActorContext());
            TBase::IsDead = true;
        }

        void DescribeTopic(NACLib::EAccessRights accessRights) {
            this->RegisterWithSameMailbox(NPQ::NDescriber::CreateDescriberActor(
                this->SelfId(),
                this->Database,
                { this->GetTopicPath() },
                {
                    .UserToken = this->GetUserToken(),
                    .AccessRights = accessRights,
                }
            ));
        }

        TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
            if (auto const& token = this->Request_->GetSerializedToken()) {
                return MakeIntrusive<NACLib::TUserToken>(token);
            }
            return nullptr;
        }

        // Requests dispatched via DoLocalRpc carry no RlPath, so the rate-limiter
        // coordination node / resource path have to be resolved from the database
        // user-attributes (Kafka-proxy does the same for its RU billing). The
        // navigate is tagged with a distinct scheme-cache cookie so the shared
        // TEvNavigateKeySetResult handler can tell it apart from the topic
        // describe navigate (which uses the default cookie 0).
        static constexpr ui64 RlPathNavigateCookie = 1;

        void SendRlPathNavigate() {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = NKikimr::SplitPath(this->Database);
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.SyncVersion = false;
            request->ResultSet.emplace_back(std::move(entry));
            request->DatabaseName = this->Database;
            request->Cookie = RlPathNavigateCookie;
            this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        }

        static bool IsRlPathNavigateResponse(const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            return ev->Get()->Request->Cookie == RlPathNavigateCookie;
        }

        // Builds a rate-limiter context from the serverless_rt_* database
        // attributes. Returns Nothing() when the attributes are absent, in which
        // case the topic is simply not RU-metered and no quota is charged.
        TMaybe<NPQ::TRlContext> ExtractRlContext(const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) const {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            if (result->ResultSet.empty()) {
                return Nothing();
            }
            const auto& entry = result->ResultSet.front();
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                return Nothing();
            }

            TString coordinationNode;
            TString resourcePath;
            if (const auto* value = entry.Attributes.FindPtr(TString(RL_COORDINATION_NODE_ATTR))) {
                coordinationNode = *value;
            }
            if (const auto* value = entry.Attributes.FindPtr(TString(RL_TOPIC_RESOURCE_ATTR))) {
                resourcePath = *value;
            }
            if (coordinationNode.empty() || resourcePath.empty()) {
                return Nothing();
            }

            return NPQ::TRlContext(coordinationNode, resourcePath, this->Database, this->Request_->GetSerializedToken());
        }
    };
}
