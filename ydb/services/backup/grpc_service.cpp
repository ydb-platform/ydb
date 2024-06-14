#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_backup.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcBackupService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::Backup::IN, Ydb::Backup::OUT, TGRpcBackupService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::Backup::IN, Ydb::Backup::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr})); \
        }, &Ydb::Backup::V1::BackupService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("backup", #NAME))->Run();

    ADD_REQUEST(FetchBackupCollections, FetchBackupCollectionsRequest, FetchBackupCollectionsResponse, DoFetchBackupCollectionsRequest);
    ADD_REQUEST(ListBackupCollections, ListBackupCollectionsRequest, ListBackupCollectionsResponse, DoListBackupCollectionsRequest);
    ADD_REQUEST(CreateBackupCollection, CreateBackupCollectionRequest, CreateBackupCollectionResponse, DoCreateBackupCollectionRequest);
    ADD_REQUEST(ReadBackupCollection, ReadBackupCollectionRequest, ReadBackupCollectionResponse, DoReadBackupCollectionRequest);
    ADD_REQUEST(UpdateBackupCollection, UpdateBackupCollectionRequest, UpdateBackupCollectionResponse, DoUpdateBackupCollectionRequest);
    ADD_REQUEST(DeleteBackupCollection, DeleteBackupCollectionRequest, DeleteBackupCollectionResponse, DoDeleteBackupCollectionRequest);

#undef ADD_REQUEST

}

} // namespace NGRpcService
} // namespace NKikimr
