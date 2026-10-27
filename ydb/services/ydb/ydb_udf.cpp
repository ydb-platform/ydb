#include "ydb_udf.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_mon.h>
#include <ydb/core/grpc_services/service_udf.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbUdfService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Udf;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#define SETUP_UDF_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, udf, auditMode, EEmptyDatabaseMode::EmptyDatabaseForbidden)

    SETUP_UDF_METHOD(DeleteModule, DoDeleteModuleRequest, RLSWITCH(Rps), UNSPECIFIED,
        TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_UDF_METHOD(ListModules, DoListModulesRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_UDF_METHOD(DescribeModule, DoDescribeModuleRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());

#undef SETUP_UDF_METHOD

    {
        using TUploadRequest = NGRpcServer::TGRpcStreamingRequest<
            UploadModuleChunk,
            UploadModuleResponse,
            TGRpcYdbUdfService,
            NKikimrServices::GRPC_SERVER>;

        TUploadRequest::Start(
            this,
            this->GetService(),
            CQ_,
            &Ydb::Udf::V1::UdfService::AsyncService::RequestUploadModule,
            [this](TIntrusivePtr<TUploadRequest::IContext> context) {
                ReportGrpcReqToMon(*ActorSystem_, context->GetPeerName());
                ActorSystem_->Send(GRpcRequestProxyId_, new TEvUploadModuleRequest(std::move(context), {
                    .AuditMode = TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin),
                    .EmptyDatabaseMode = EEmptyDatabaseMode::EmptyDatabaseForbidden,
                }));
            },
            *ActorSystem_,
            "UploadModule",
            getCounterBlock("udf", "UploadModule", true),
            nullptr);
    }
}

} // namespace NGRpcService
} // namespace NKikimr
