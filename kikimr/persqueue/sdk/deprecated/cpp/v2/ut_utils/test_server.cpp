#include "test_server.h"

#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <kikimr/yndx/grpc_services/persqueue/persqueue.h>
#include <kikimr/yndx/persqueue/msgbus_server/read_session_info.h>

namespace NPersQueue {

const TVector<NKikimrServices::EServiceKikimr> TTestServer::LOGGED_SERVICES = {
    NKikimrServices::PERSQUEUE,
    NKikimrServices::PQ_METACACHE,
    NKikimrServices::PQ_READ_PROXY,
    NKikimrServices::PQ_WRITE_PROXY,
    NKikimrServices::PQ_MIRRORER,
};

void TTestServer::PatchServerSettings() {
    ServerSettings.RegisterGrpcService<NKikimr::NGRpcService::TGRpcPersQueueService>(
        "pq",
        NKikimr::NMsgBusProxy::CreatePersQueueMetaCacheV2Id()
    );
    ServerSettings.SetPersQueueGetReadSessionsInfoWorkerFactory(
        std::make_shared<NKikimr::NMsgBusProxy::TPersQueueGetReadSessionsInfoWorkerWithPQv0Factory>()
    );
}

void TTestServer::StartIfNeeded(bool start) {
    if (start) {
        StartServer();
    }
}



} // namespace NPersQueue
