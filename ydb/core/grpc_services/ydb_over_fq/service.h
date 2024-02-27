#pragma once

#include "ydb/core/grpc_services/base/base.h"
#include "ydb/library/actors/core/actor_bootstrapped.h"
#include <memory>

namespace NKikimr::NGRpcService {

class IRequestOpCtx;
class IRequestNoOpCtx;
class IFacilityProvider;

namespace NYdbOverFq {

class TRequestActor : public NActors::TActorBootstrapped<TRequestActor> {
public:
    TRequestActor(std::unique_ptr<IRequestCtx> request)
        : Request{std::move(request)} {}

    void Bootstrap() {
        auto now = TInstant::Now();
        const auto& deadline = Request->GetDeadline();

        if (deadline <= now) {
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                SelfId() << " Request deadline has expired for " << now - deadline << " seconds");

            Reply(Ydb::StatusIds::TIMEOUT);
            return;
        }
    }

protected:
    void Reply(Ydb::StatusIds::StatusCode status) {
        Request->ReplyWithYdbStatus(status);
        this->PassAway();
    }

private:
    std::unique_ptr<IRequestCtx> Request;

};

// table
void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoKeepAliveRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoDescribeTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&);
void DoExplainDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
// void DoPrepareDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);
void DoListDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f);

// scheme


void DoExecuteQuery(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoExecuteScript(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoFetchScriptResults(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&);
void DoCreateSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoDeleteSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoAttachSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoBeginTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoCommitTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);
void DoRollbackTransaction(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f);

} // namespace NYdbOverFq

} // namespace NKikimr::NGRpcService
