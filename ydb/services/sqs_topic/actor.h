#pragma once

#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/library/http_proxy/error/error.h>

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
    };
}
