#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>

#include <util/random/shuffle.h>

#include <ydb/core/health_check/health_check.h> 

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

class TSelfCheckRPC : public TRpcRequestActor<TSelfCheckRPC, TEvSelfCheckRequest> {
public:
    using TRpcRequestActor::TRpcRequestActor;

    THolder<NHealthCheck::TEvSelfCheckResult> Result;
    Ydb::StatusIds_StatusCode Status = Ydb::StatusIds::SUCCESS;

    void Bootstrap() {
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        request->Request = *(Request->GetProtoRequest());
        if (Request->GetDatabaseName()) {
            request->Database = Request->GetDatabaseName().GetRef();
        }
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        Become(&TThis::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
        }
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        Status = Ydb::StatusIds::SUCCESS;
        Result = ev->Release();
        ReplyAndPassAway();
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        Status = Ydb::StatusIds::UNAVAILABLE;
        ReplyAndPassAway();
    }

    void ReplyAndPassAway() {
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Status);
        if (Result) {
            operation.mutable_result()->PackFrom(Result->Result);
        }
        return Reply(response);
    }
};

void TGRpcRequestProxy::Handle(TEvSelfCheckRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TSelfCheckRPC(ev->Release().Release()));
}

} // namespace NGRpcService
} // namespace NKikimr
