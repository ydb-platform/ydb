#pragma once

#include "billing.h"
#include "error.h"
#include "statuses.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/ru_quoter/ru_quoter.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/library/actors/core/events.h>

#include <util/generic/algorithm.h>
#include <util/system/type_name.h>
#include <util/system/backtrace.h>

namespace NKikimr::NSqsTopic::V1 {

    template<class TDerived, class TRequest>
    class TGrpcActorBase
        : public NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest>
    {
        public:
        using TBase = NGRpcProxy::V1::TPQGrpcSchemaBase<TDerived, TRequest>;
        using TBase::TopicPath;

        TGrpcActorBase(NKikimr::NGRpcService::IRequestOpCtx* request, const TString& topicPath)
            : TBase(request, topicPath)
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
            if (TBase::IsDead) {
                return;
            }
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
            NACLib::TUserToken token(this->Request_->GetSerializedToken());
            ShouldBeCharged_ = FindPtr(AppData(ctx)->PQConfig.GetNonChargeableUser(), token.GetUserSID()) == nullptr;
        }

        void Die(const NActors::TActorContext& ctx) override {
            if (QuoterActorId_) {
                ctx.Send(QuoterActorId_, new NActors::TEvents::TEvPoison);
                QuoterActorId_ = {};
            }
            TBase::Die(ctx);
        }

        void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NPQ::NDescriber::TEvDescribeTopicsResponse, HandleDescribeTopicsResponse);
                hFunc(NPQ::NRuQuoter::TEvChargeRequestUnitsResponse, HandleChargeRequestUnitsResponse);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleUnexpectedNavigate);
                default:
                    TBase::StateWork(ev);
            }
        }

        // Invoked after the topic is described so that payload-metered methods can
        // see fifo / PQ config. The returned amount is acquired as quota and written
        // into the yds bill.
        virtual ui64 GetRUCost() = 0;

        TTopicDescribePolicy GetTopicDescribePolicy() const {
            return ExistingQueuePolicy();
        }

        void OnTopicDescribed(const NPQ::NDescriber::TTopicInfo&) {
        }

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&) {
        }

        void ChargeRequestUnits(const NActors::TActorContext& ctx) {
            if (TBase::IsDead) {
                return;
            }
            if (!ShouldBeCharged_) {
                return static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
            }
            const ui64 ru = this->GetRUCost();
            if (ru == 0) {
                return static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
            }
            AFL_ENSURE(!QuoterActorId_);
            QuoterActorId_ = this->RegisterWithSameMailbox(NPQ::NRuQuoter::CreateRequestUnitsQuoter(
                this->SelfId(),
                NPQ::NRuQuoter::TRequestUnitsQuoterSettings{
                    .Database = this->Database,
                    .Ru = ru,
                    .Token = this->Request_->GetSerializedToken(),
                }));
        }

        // Called after quota is granted (or skipped) and the bill is written.
        void OnRequestUnitsCharged(const NActors::TActorContext&) {
        }

    protected:
        bool ShouldBeCharged_ = false;

    private:
        void HandleDescribeTopicsResponse(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr& ev) {
            const auto* topicInfo = TakeSingleTopic(*ev->Get());
            if (!topicInfo) {
                ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE, "Failed to describe topic"));
                return;
            }
            const TTopicDescribePolicy policy = static_cast<TDerived*>(this)->GetTopicDescribePolicy();
            if (auto error = MapTopicInfoToSqsError(this->GetTopicPath(), *topicInfo, policy)) {
                ReplyWithError(*error);
                return;
            }
            static_cast<TDerived*>(this)->OnTopicDescribed(*topicInfo);
        }

        void HandleChargeRequestUnitsResponse(NPQ::NRuQuoter::TEvChargeRequestUnitsResponse::TPtr& ev) {
            QuoterActorId_ = {};
            if (TBase::IsDead) {
                return;
            }
            const auto& ctx = TlsActivationContext->AsActorContext();
            switch (ev->Get()->Status) {
                case NPQ::NRuQuoter::EStatus::SUCCESS:
                    static_cast<TDerived*>(this)->OnRequestUnitsCharged(ctx);
                    return;
                case NPQ::NRuQuoter::EStatus::THROTTLED:
                    ReplyWithError(MakeError(NSQS::NErrors::THROTTLING_EXCEPTION,
                        ev->Get()->Message.empty()
                            ? NPQ::NRuQuoter::Description(ev->Get()->Status)
                            : ev->Get()->Message));
                    return;
                case NPQ::NRuQuoter::EStatus::UNKNOWN_ERROR:
                    ReplyWithError(MakeError(NSQS::NErrors::INTERNAL_FAILURE,
                        ev->Get()->Message.empty()
                            ? NPQ::NRuQuoter::Description(ev->Get()->Status)
                            : ev->Get()->Message));
                    return;
            }
        }

        // Do not forward leftover scheme-cache navigates to TPQSchemaBase::Handle():
        // that would reply with PQ SCHEME_ERROR / ACCESS_DENIED instead of SQS codes.
        void HandleUnexpectedNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&) {
        }

        NActors::TActorId QuoterActorId_;
    };
}
