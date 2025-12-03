#include "ydb_tablet.h"

#include <ydb/core/grpc_services/tablet/service_tablet.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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
    using namespace Ydb::Tablet;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    size_t proxyCounter = 0;

#ifdef SETUP_TABLET_METHOD
#error SETUP_TABLET_METHOD macro already defined
#endif

#define SETUP_TABLET_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    for (size_t i = 0; i < HandlersPerCompletionQueue; ++i) {                          \
        for (auto* cq: CQS) {                                                          \
            SETUP_RUNTIME_EVENT_METHOD(                                                \
                methodName,                                                            \
                YDB_API_DEFAULT_REQUEST_TYPE(methodName),                              \
                YDB_API_DEFAULT_RESPONSE_TYPE(methodName),                             \
                methodCallback,                                                        \
                rlMode,                                                                \
                requestType,                                                           \
                YDB_API_DEFAULT_COUNTER_BLOCK(tablet, methodName),                     \
                auditMode,                                                             \
                COMMON,                                                                \
                ::NKikimr::NGRpcService::TGrpcRequestNoOperationCall,                  \
                GRpcProxies_[proxyCounter % GRpcProxies_.size()],                      \
                cq,                                                                    \
                nullptr,                                                               \
                nullptr                                                                \
            );                                                                         \
            ++proxyCounter;                                                            \
        }                                                                              \
    }

    SETUP_TABLET_METHOD(ExecuteTabletMiniKQL, DoExecuteTabletMiniKQLRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_TABLET_METHOD(ChangeTabletSchema, DoChangeTabletSchemaRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));
    SETUP_TABLET_METHOD(RestartTablet, DoRestartTabletRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::ClusterAdmin));

#undef SETUP_TABLET_METHOD
}

} // namespace NKikimr::NGRpcService
