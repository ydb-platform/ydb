#include "db_metadata_cache.h"
#include "service_monitoring.h"

#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/viewer/viewer.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>

#include <util/random/shuffle.h>

#include <ydb/core/health_check/health_check.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvClusterStateRequest = TGrpcRequestOperationCall<Ydb::Monitoring::ClusterStateRequest, Ydb::Monitoring::ClusterStateResponse>;

class TClusterStateRPC : public TRpcRequestActor<TClusterStateRPC, TEvClusterStateRequest, true> {
public:
    using TRpcRequestActor::TRpcRequestActor;

    TString CountersResult;
    Ydb::StatusIds_StatusCode Status = Ydb::StatusIds::SUCCESS;

    void SendClusterStateRequests() {
        NMonitoring::TMonService2HttpRequest monReq(nullptr, nullptr, nullptr, nullptr, "/counters", nullptr);
        auto request = MakeHolder<NMon::TEvHttpInfo>(monReq);
        Send(NKikimr::NViewer::MakeViewerID(0), request.Release());

        Become(&TThis::StateWaitResponses);
    }

    void Bootstrap() {
        SendClusterStateRequests();
        ui32 duration = 60;
        if (GetProtoRequest()->duration()) {
            duration = GetProtoRequest()->duration();
        }
        Schedule(TDuration::Seconds(duration), new TEvents::TEvWakeup());
    }

    void Timeout() {

        ReplyAndPassAway();
    }

    STATEFN(StateWaitResponses) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfoRes, Handle);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr& ev) {
        Status = Ydb::StatusIds::SUCCESS;
        NMon::TEvHttpInfoRes *ptr = dynamic_cast<NMon::TEvHttpInfoRes*>(ev->Get());
        CountersResult = std::move(ptr->Answer);
        ReplyAndPassAway();
    }

    void ReplyAndPassAway() {
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Status);
        Ydb::Monitoring::ClusterStateResult result;
        TStringBuilder res;
        res << "{\"Counters\": {\n";
        res << CountersResult;
        res << "}}";
        result.Setresult(res);
        operation.mutable_result()->PackFrom(result);
        return Reply(response);
    }
};

void DoClusterStateRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TClusterStateRPC(p.release()));
}
} // namespace NGRpcService
} // namespace NKikimr
