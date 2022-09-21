#include "ydb_logstore.h"

#include <ydb/core/grpc_services/service_logstore.h>
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/grpc_helper.h>

namespace NKikimr::NGRpcService {

void TGRpcYdbLogStoreService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    using namespace Ydb;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB) \
    MakeIntrusive<TGRpcRequest<LogStore::NAME##Request, LogStore::NAME##Response, TGRpcYdbLogStoreService>> \
        (this, &Service_, CQ_, [this](NGrpc::IRequestContextBase *ctx) {                                    \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
            ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                new TGrpcRequestOperationCall<LogStore::NAME##Request, LogStore::NAME##Response>            \
                    (ctx, &CB));                                                                            \
        }, &Ydb::LogStore::V1::LogStoreService::AsyncService::Request ## NAME,                              \
        #NAME, logger, getCounterBlock("logstore", #NAME))->Run();

    ADD_REQUEST(CreateLogStore, DoCreateLogStoreRequest)
    ADD_REQUEST(DescribeLogStore, DoDescribeLogStoreRequest)
    ADD_REQUEST(DropLogStore, DoDropLogStoreRequest)
    ADD_REQUEST(AlterLogStore, DoAlterLogStoreRequest)

    ADD_REQUEST(CreateLogTable, DoCreateLogTableRequest)
    ADD_REQUEST(DescribeLogTable, DoDescribeLogTableRequest)
    ADD_REQUEST(DropLogTable, DoDropLogTableRequest)
    ADD_REQUEST(AlterLogTable, DoAlterLogTableRequest)

#undef ADD_REQUEST
}

}
