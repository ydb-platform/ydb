#include "ydb_tablet.h"

#include <ydb/core/grpc_services/tablet/service_tablet.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcService {

TGRpcYdbTabletService::TGRpcYdbTabletService(
        NActors::TActorSystem *system,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TVector<NActors::TActorId>& proxies,
        bool rlAllowed,
        size_t handlersPerCompletionQueue)
    : TBase(system, counters, proxies, rlAllowed)
    , HandlersPerCompletionQueue(handlersPerCompletionQueue)
{}

void TGRpcYdbTabletService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    size_t proxyCounter = 0;

#ifdef ADD_REQUEST_LIMIT
#error ADD_REQUEST_LIMIT macro already defined
#endif

#define ADD_REQUEST_LIMIT(NAME, CB, LIMIT_TYPE, ...) do {                                                               \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                                                           \
        for (auto* cq: CQS) {                                                                                           \
            auto proxy = GRpcProxies_[proxyCounter++ % GRpcProxies_.size()];                                            \
            MakeIntrusive<TGRpcRequest<Ydb::Tablet::NAME##Request, Ydb::Tablet::NAME##Response, TGRpcYdbTabletService>> \
                (this, &Service_, cq,                                                                                   \
                    [this, proxy](NYdbGrpc::IRequestContextBase *ctx) {                                                 \
                        NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());                                \
                        ActorSystem_->Send(proxy,                                                                       \
                            new TGrpcRequestNoOperationCall<Ydb::Tablet::NAME##Request, Ydb::Tablet::NAME##Response>    \
                                (ctx, &CB, TRequestAuxSettings {                                                        \
                                    .RlMode = RLSWITCH(TRateLimiterMode::LIMIT_TYPE),                                   \
                                    __VA_OPT__(.AuditMode = TAuditMode::__VA_ARGS__,)                                   \
                                }));                                                                                    \
                    }, &Ydb::Tablet::V1::TabletService::AsyncService::Request ## NAME,                                  \
                    #NAME, logger, getCounterBlock("tablet", #NAME))->Run();                                            \
        }                                                                                                               \
    }                                                                                                                   \
} while(0)

    ADD_REQUEST_LIMIT(ExecuteTabletMiniKQL, DoExecuteTabletMiniKQLRequest, Rps, Auditable);
    ADD_REQUEST_LIMIT(ChangeTabletSchema, DoChangeTabletSchemaRequest, Rps, Auditable);
    ADD_REQUEST_LIMIT(RestartTablet, DoRestartTabletRequest, Rps);

#undef ADD_REQUEST_LIMIT
}

} // namespace NKikimr::NGRpcService
