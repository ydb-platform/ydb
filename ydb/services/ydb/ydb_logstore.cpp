#include "ydb_logstore.h"

#include <ydb/core/grpc_services/service_logstore.h>
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/grpc_helper.h>

namespace NKikimr::NGRpcService {

void TGRpcYdbLogStoreService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, AUDIT_MODE)                                                                   \
    MakeIntrusive<TGRpcRequest<LogStore::NAME##Request, LogStore::NAME##Response, TGRpcYdbLogStoreService>> \
        (this, &Service_, CQ_, [this](NYdbGrpc::IRequestContextBase *ctx) {                                 \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
            ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                new TGrpcRequestOperationCall<LogStore::NAME##Request, LogStore::NAME##Response>            \
                    (ctx, &CB, TRequestAuxSettings{.AuditMode = AUDIT_MODE}));                              \
        }, &Ydb::LogStore::V1::LogStoreService::AsyncService::Request ## NAME,                              \
        #NAME, logger, getCounterBlock("logstore", #NAME))->Run();

    ADD_REQUEST(CreateLogStore, DoCreateLogStoreRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(DescribeLogStore, DoDescribeLogStoreRequest, TAuditMode::NonModifying())
    ADD_REQUEST(DropLogStore, DoDropLogStoreRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(AlterLogStore, DoAlterLogStoreRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))

    ADD_REQUEST(CreateLogTable, DoCreateLogTableRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(DescribeLogTable, DoDescribeLogTableRequest, TAuditMode::NonModifying())
    ADD_REQUEST(DropLogTable, DoDropLogTableRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))
    ADD_REQUEST(AlterLogTable, DoAlterLogTableRequest, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl))

#undef ADD_REQUEST
}

}
