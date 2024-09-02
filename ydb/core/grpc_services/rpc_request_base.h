#pragma once

#include "grpc_request_proxy.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

template <typename TDerived, typename TEvRequest, bool HasOperation = false>
class TRpcRequestActor: public TActorBootstrapped<TDerived> {
public:
    using TRequest = typename TEvRequest::TRequest;
    using TResponse = typename TEvRequest::TResponse;
    using TOperation = Ydb::Operations::Operation;
    typedef typename std::conditional<HasOperation, IRequestOpCtx, IRequestNoOpCtx>::type TRequestCtx;

protected:
    void Reply(
        const TResponse& response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS
    ) {
        TProtoResponseHelper::SendProtoResponse(response, status, Request);
        this->PassAway();
    }

    void Reply(
        const Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues
    ) {
        TResponse response;
        if constexpr (HasOperation) {
            *response.mutable_operation() = MakeOperation(status, issues);
        } else {
            response.set_status(status);
            if (issues.size()) {
                response.mutable_issues()->CopyFrom(issues);
            }
        }
        Reply(response, status);
    }

    void Reply(const Ydb::StatusIds::StatusCode status, const TString& error = TString()) {
        Reply(status, ErrorToIssues(error));
    }

    void Reply(
        const Ydb::StatusIds::StatusCode status,
        NKikimrIssues::TIssuesIds::EIssueCode code,
        const TString& error
    ) {
        Request->RaiseIssue(MakeIssue(code, error));
        Reply(status, error);
    }

    void Reply(const Ydb::StatusIds::StatusCode status, NKikimrIssues::TIssuesIds::EIssueCode code) {
        Request->RaiseIssue(MakeIssue(code));
        Reply(status);
    }

    void Reply(const Ydb::Operations::Operation& operation) {
        Request->SendOperation(operation);
        this->PassAway();
    }

    bool CheckAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject, ui32 access) {
        if (!UserToken || !securityObject) {
            return true;
        }

        if (securityObject->CheckAccess(access, *UserToken)) {
            return true;
        }

        Reply(Ydb::StatusIds::UNAUTHORIZED, NKikimrIssues::TIssuesIds::ACCESS_DENIED,
            TStringBuilder() << "Access denied"
                << ": for# " << UserToken->GetUserSID()
                << ", path# " << path
                << ", access# " << NACLib::AccessRightsToString(access));
        return false;
    }

    template <typename TResult, typename TMetadata>
    static TOperation MakeOperation(
        const TString& id,
        const Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues,
        const TResult& result, const TMetadata& metadata
    ) {
        TOperation operation = MakeOperationBase(id, true, status, issues);
        operation.mutable_result()->PackFrom(result);
        operation.mutable_metadata()->PackFrom(metadata);
        return operation;
    }

    static TOperation MakeOperation(
        const Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues
    ) {
        return MakeOperationBase(TString(), true, status, issues);
    }

    static TOperation MakeOperation(const Ydb::StatusIds::StatusCode status, const TString& error = TString()) {
        return MakeOperation(status, ErrorToIssues(error));
    }

    const typename TEvRequest::TRequest* GetProtoRequest() const {
        return TEvRequest::GetProtoRequest(Request);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TRpcRequestActor(TRequestCtx* ev)
        : Request(ev)
        , UserToken(CreateUserToken(Request.Get()))
    {
    }

private:
    static THolder<const NACLib::TUserToken> CreateUserToken(TRequestCtx* request) {
        if (const auto& userToken = request->GetSerializedToken()) {
            return MakeHolder<NACLib::TUserToken>(userToken);
        } else {
            return {};
        }
    }

private:
    static google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> ErrorToIssues(const TString& error) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;

        if (error) {
            auto& issue = *issues.Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(error);
        }

        return issues;
    }

    static TOperation MakeOperationBase(
        const TString& id, const bool ready,
        const Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues
    ) {
        TOperation operation;
        operation.set_id(id);
        operation.set_ready(ready);
        operation.set_status(status);
        if (issues.size()) {
            operation.mutable_issues()->CopyFrom(issues);
        }
        return operation;
    }

protected:
    const TString& GetDatabaseName() const {
        if (!DatabaseName_.has_value()) {
            auto name = Request->GetDatabaseName();
            if (name) {
                DatabaseName_.emplace(std::move(*name));
            } else {
                DatabaseName_.emplace(DatabaseFromDomain(AppData()));
            }
        }
        return *DatabaseName_;
    }

protected:
    const THolder<TRequestCtx> Request;
    const THolder<const NACLib::TUserToken> UserToken;

private:
    mutable std::optional<TString> DatabaseName_;

}; // TRpcRequestActor

} // namespace NGRpcService
} // namespace NKikimr
