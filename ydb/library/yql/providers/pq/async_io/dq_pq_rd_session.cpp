#include "dq_pq_rd_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
//#include <ydb/core/fq/libs/row_dispatcher/leader_detector.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>


#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/join.h>

#include <queue>
#include <variant>

#define SRC_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

class PqSession : public NActors::TActorBootstrapped<PqSession>{

private: 
    ui32 PartitionId;
    const TString LogPrefix;

public:
    PqSession(const NPq::NProto::TDqPqTopicSource& /*settings*/, ui32 partitionId);

  //  void Handle(NFq::TEvRowDispatcher::TEvRowDispatcherResult::TPtr &ev);
   // void Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr &ev);


    STRICT_STFUNC(
        StateFunc, {
      //  hFunc(NFq::TEvRowDispatcher::TEvRowDispatcherResult, Handle);
      //  hFunc(NFq::TEvRowDispatcher::TEvCoordinatorResult, Handle);
        // hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        // hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        // hFunc(TEvents::TEvUndelivered, Handle);
        // hFunc(NActors::TEvents::TEvWakeup, Handle)
    })
    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void Bootstrap();
};

PqSession::PqSession(const NPq::NProto::TDqPqTopicSource& /*settings*/, ui32 partitionId)
    : PartitionId(partitionId)
    , LogPrefix(TStringBuilder() << "PQ RD source [" << PartitionId << "]. ")

{
    
}

void PqSession::Bootstrap() {
    Become(&PqSession::StateFunc);
    SRC_LOG_D("PqSession::Bootstrap partitionId " << PartitionId);

}

std::unique_ptr<NActors::IActor> NewPqSession(const NPq::NProto::TDqPqTopicSource& settings, ui32 partitionId) {
    return std::unique_ptr<NActors::IActor>(new PqSession(settings, partitionId));
}

} // namespace NYql::NDq
