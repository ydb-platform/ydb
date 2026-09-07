#pragma once

#include "billing.h"
#include "error.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/library/actors/core/events.h>

#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/system/type_name.h>
#include <util/system/backtrace.h>

namespace NKikimr::NSqsTopic::V1 {

    // Database user-attribute keys that carry the rate-limiter (RU billing)
    // coordination node and topic resource paths. Kept in sync with the gRPC
    // request check actor (ruRlTopicConfig).
    inline constexpr TStringBuf RL_COORDINATION_NODE_ATTR = "serverless_rt_coordination_node_path";
    inline constexpr TStringBuf RL_TOPIC_RESOURCE_ATTR = "serverless_rt_topic_resource_ru";
    inline constexpr TStringBuf CLOUD_ID_ATTR = "cloud_id";
    inline constexpr TStringBuf FOLDER_ID_ATTR = "folder_id";
    inline constexpr TStringBuf DATABASE_ID_ATTR = "database_id";

    template<class TDerived, class TRequest>
    class TGrpcActorBase
        : public NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest>
        , protected NPQ::TRlHelpers
    {
        public:
        using TBase = NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest>;
        using EWakeupTag = NPQ::TRlHelpers::EWakeupTag;
        using TBase::TopicPath;

        TGrpcActorBase(NKikimr::NGRpcService::IRequestOpCtx* request, const TString& topicPath,
                       ui64 ruBlockSize = NBilling::WRITE_BLOCK_SIZE)
            : TBase(request, topicPath)
            , NPQ::TRlHelpers({}, request, ruBlockSize, false)
        {
        }

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

        bool OnUnhandledException(const std::exception& exc) override {
            const auto& ctx = this->ActorContext();
            YDB_LOG_CRIT_CTX_COMP(ctx, NKikimrServices::SQS, "Unhandled exception in SQS topic actor",
                {"typeName", TypeName(exc)},
                {"exception", exc.what()},
                {"path", this->GetTopicPath()},
                {"database", this->Database},
                {"backTrace", TBackTrace::FromCurrentException().PrintToString()});

            ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Internal error"));
            return true;
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

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            NPQ::TRlHelpers::Bootstrap(this->SelfId(), ctx);
            NACLib::TUserToken token(this->Request_->GetSerializedToken());
            ShouldBeCharged_ = FindPtr(AppData(ctx)->PQConfig.GetNonChargeableUser(), token.GetUserSID()) == nullptr;
            if (ShouldBeCharged_) {
                SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
            }
        }

        void Die(const NActors::TActorContext& ctx) override {
            NPQ::TRlHelpers::PassAway(this->SelfId());
            TBase::Die(ctx);
        }

        void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
                hFunc(NActors::TEvents::TEvWakeup, HandleBillingWakeup);
                default:
                    TBase::StateWork(ev);
            }
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
        // case quota is not acquired (the yds bill may still be written).
        TMaybe<NPQ::TRlContext> ExtractRlContext(const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) const {
            const auto* entry = GetNavigateEntry(ev);
            if (!entry) {
                return Nothing();
            }

            TString coordinationNode;
            TString resourcePath;
            if (const auto* value = entry->Attributes.FindPtr(TString(RL_COORDINATION_NODE_ATTR))) {
                coordinationNode = *value;
            }
            if (const auto* value = entry->Attributes.FindPtr(TString(RL_TOPIC_RESOURCE_ATTR))) {
                resourcePath = *value;
            }
            if (coordinationNode.empty() || resourcePath.empty()) {
                return Nothing();
            }

            return NPQ::TRlContext(coordinationNode, resourcePath, this->Database, this->Request_->GetSerializedToken());
        }

        NBilling::TMeteringIds ExtractMeteringIds(const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) const {
            NBilling::TMeteringIds ids;
            const auto* entry = GetNavigateEntry(ev);
            if (!entry) {
                return ids;
            }
            if (const auto* value = entry->Attributes.FindPtr(TString(CLOUD_ID_ATTR))) {
                ids.CloudId = *value;
            }
            if (const auto* value = entry->Attributes.FindPtr(TString(FOLDER_ID_ATTR))) {
                ids.FolderId = *value;
            }
            if (const auto* value = entry->Attributes.FindPtr(TString(DATABASE_ID_ATTR))) {
                ids.DatabaseId = *value;
            }
            return ids;
        }

        void WriteChargedRequestUnits(const NActors::TActorContext& ctx) {
            if (!MeteringIds_.IsComplete() || ChargedRu_ == 0) {
                return;
            }
            NMetering::SendMeteringJson(
                ctx,
                NBilling::MakeRequestUnitsBill(MeteringIds_, ChargedRu_, ctx.Now(), CreateGuidAsString()));
        }

        // Invoked after the serverless RL path is resolved so that
        // CalcRuConsumption() can see the kesus resource. The returned amount
        // is acquired as quota and written into the yds bill.
        virtual ui64 GetRUCost() = 0;

        void ChargeRequestUnits(const NActors::TActorContext& ctx) {
            if (!ShouldBeCharged_) {
                return static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
            }
            SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
            if (!RlPathResolved_) {
                PendingCharge_ = true;
                SendRlPathNavigate();
                return;
            }
            AcquireQuotaForComputedCost(ctx);
        }

        // Called after quota is granted (or skipped) and the bill is written.
        void OnRequestUnitsCharged(const NActors::TActorContext&) {
        }

    protected:
        NBilling::TMeteringIds MeteringIds_;
        ui64 ChargedRu_ = 0;
        bool ShouldBeCharged_ = false;

    private:
        static const NSchemeCache::TSchemeCacheNavigate::TEntry* GetNavigateEntry(
            const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev)
        {
            const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
            if (result->ResultSet.empty()) {
                return nullptr;
            }
            const auto& entry = result->ResultSet.front();
            if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                return nullptr;
            }
            return &entry;
        }

        void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            if (IsRlPathNavigateResponse(ev)) {
                HandleRlPathNavigate(ev);
                return;
            }
            static_cast<TDerived*>(this)->HandleCacheNavigateResponse(ev);
        }

        void HandleRlPathNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
            MeteringIds_ = ExtractMeteringIds(ev);
            if (auto rlContext = ExtractRlContext(ev)) {
                SetRlContext(*rlContext);
            }
            RlPathResolved_ = true;

            const auto& ctx = TlsActivationContext->AsActorContext();
            if (PendingCharge_) {
                PendingCharge_ = false;
                AcquireQuotaForComputedCost(ctx);
                return;
            }
        }

        void AcquireQuotaForComputedCost(const NActors::TActorContext& ctx) {
            ChargedRu_ = this->GetRUCost();
            if (ChargedRu_ == 0) {
                // Zero cost: treat quota as already granted. Do not talk to the
                // rate limiter and do not write a bill.
                static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
                return;
            }
            if (IsQuotaRequired()) {
                AFL_ENSURE(MaybeRequestQuota(ChargedRu_, EWakeupTag::RlAllowed, ctx))
                    ("ru", ChargedRu_);
                return;
            }
            WriteChargedRequestUnits(ctx);
            static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
        }

        void HandleBillingWakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
            const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
            const auto& ctx = TlsActivationContext->AsActorContext();
            switch (tag) {
                case EWakeupTag::RlAllowed:
                    NPQ::TRlHelpers::OnWakeup(tag);
                    WriteChargedRequestUnits(ctx);
                    static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
                    return;
                case EWakeupTag::RlNoResource:
                    NPQ::TRlHelpers::OnWakeup(tag);
                    ReplyWithError(MakeError(NSQS::NErrors::THROTTLING_EXCEPTION, "Request was throttled by the rate limiter"));
                    return;
                default:
                    NPQ::TRlHelpers::OnWakeup(tag);
                    return;
            }
        }

        bool RlPathResolved_ = false;
        bool PendingCharge_ = false;
    };
}
