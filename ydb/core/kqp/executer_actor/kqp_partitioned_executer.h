#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/aclib/aclib.h>

#include <yql/essentials/core/pg_settings/guc_settings.h>

namespace NKikimr::NKqp {

struct TKqpPartitionedExecuterSettings {
    IKqpGateway::TExecPhysicalRequest&& Request;
    TActorId SessionActorId;
    const NMiniKQL::IFunctionRegistry* FuncRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken>& UserToken;
    TKqpRequestCounters::TPtr RequestCounters;
    const TExecuterConfig& ExecuterConfig;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
    const TIntrusivePtr<TUserRequestContext>& UserRequestContext;
    ui32 StatementResultIndex;
    std::optional<TKqpFederatedQuerySetup>& FederatedQuerySetup;
    const TGUCSettings::TPtr& GUCSettings;
    const TShardIdToTableInfoPtr& ShardIdToTableInfo;
    ui64 WriteBufferInitialMemoryLimit;
    ui64 WriteBufferMemoryLimit;
    ui64 QuerySpanId = 0;
};

NActors::IActor* CreateKqpPartitionedExecuter(TKqpPartitionedExecuterSettings settings, std::shared_ptr<NYql::NDq::IDqChannelService> channelService);

}  // namespace NKikimr::NKqp
