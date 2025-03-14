#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/aclib/aclib.h>

#include <yql/essentials/core/pg_settings/guc_settings.h>

namespace NKikimr::NKqp {

#define PE_LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)
#define PE_LOG_T(msg) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, LogPrefix() << msg)

constexpr ui64 MaxLimitSize = 10000;
constexpr ui64 MinLimitSize = 1;

NActors::IActor* CreateKqpPartitionedExecuter(
    NKikimr::NKqp::IKqpGateway::TExecPhysicalRequest&& literalRequest, NKikimr::NKqp::IKqpGateway::TExecPhysicalRequest&& physicalRequest,
    const TActorId sessionActorId, const TString& database, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
    const TIntrusivePtr<NKikimr::NKqp::TKqpCounters>& counters, NKikimr::NKqp::TKqpRequestCounters::TPtr requestCounters,
    const NKikimrConfig::TTableServiceConfig& tableServiceConfig, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TPreparedQueryHolder::TConstPtr preparedQuery, const TIntrusivePtr<NKikimr::NKqp::TUserRequestContext>& userRequestContext,
    ui32 statementResultIndex, const std::optional<NKikimr::NKqp::TKqpFederatedQuerySetup>& federatedQuerySetup,
    const TGUCSettings::TPtr& GUCSettings, const NKikimr::NKqp::TShardIdToTableInfoPtr& shardIdToTableInfo);

}  // namespace NKikimr::NKqp
