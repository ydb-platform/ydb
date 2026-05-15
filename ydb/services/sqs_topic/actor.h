#pragma once

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKikimr::NSqsTopic::V1 {

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
    };
}
