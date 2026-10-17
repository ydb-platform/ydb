#pragma once

#include "kqp_worker_settings.h"

#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/common/kqp_tx_manager.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/runtime/dq_channel_service.h>

#include <ydb/core/control/lib/immediate_control_board_wrapper.h>
#include <ydb/library/actors/core/actorid.h>

namespace NACLib {
    class TUserContext;
}

namespace NKikimr::NKqp::NComputeActor {
    struct IKqpNodeComputeActorFactory;
}

namespace NKikimr::NKqp::NRm {
    class IKqpResourceManager;
}

namespace NKikimr::NKqp {

class TKqpQueryCache;

IActor* CreateKqpSessionActor(const TActorId& owner,
    TIntrusivePtr<TKqpQueryCache> queryCache,
    std::shared_ptr<NKikimr::NKqp::NRm::IKqpResourceManager> resourceManager_,
    std::shared_ptr<NKikimr::NKqp::NComputeActor::IKqpNodeComputeActorFactory> caFactory_,
    const TString& sessionId,
    TIntrusiveConstPtr<NYql::TKikimrConfiguration> kqpConfig,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const TActorId& kqpTempTablesAgentActor,
    std::shared_ptr<NYql::NDq::IDqChannelService> channelService,
    TIntrusivePtr<NACLib::TUserContext> userCtx);

IActor* CreateKqpTempTablesManager(
    TKqpTempTablesState tempTablesState, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TActorId& target, const TString& database);

}  // namespace NKikimr::NKqp
