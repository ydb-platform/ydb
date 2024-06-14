#include "dq_pq_rd_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>


#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>

#include <util/generic/utility.h>

#include <ydb/library/actors/core/interconnect.h>


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
    const NPq::NProto::TDqPqTopicSource SourceParams;
    ui32 PartitionId;
    TMaybe<ui64> ReadOffset;
    TActorId RowDispatcherActorId;
    const TString LogPrefix;
    const TString Token;
    bool AddBearerToToken;

public:
    PqSession(
        const NPq::NProto::TDqPqTopicSource& sourceParams,
        ui32 partitionId,
        TActorId rowDispatcherActorId,
        const TString& token,
        bool addBearerToToken,
        TMaybe<ui64> readOffset);

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev);


    STRICT_STFUNC(
        StateFunc, {
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        // hFunc(TEvents::TEvUndelivered, Handle);
        // hFunc(NActors::TEvents::TEvWakeup, Handle)
    })
    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void Bootstrap();
};

PqSession::PqSession(
    const NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    TActorId rowDispatcherActorId,
    const TString& token,
    bool addBearerToToken,
    TMaybe<ui64> readOffset)
    : SourceParams(sourceParams)
    , PartitionId(partitionId)
    , ReadOffset(readOffset)
    , RowDispatcherActorId(rowDispatcherActorId)
    , LogPrefix(TStringBuilder() << "PQ RD source [" << PartitionId << "]. ")
    , Token(token)
    , AddBearerToToken(addBearerToToken) {
}

void PqSession::Bootstrap() {
    Become(&PqSession::StateFunc);
    SRC_LOG_D("PqSession::Bootstrap partitionId " << PartitionId);

    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStartSession>();

    event->Record.MutableSource()->CopyFrom(SourceParams);
    event->Record.SetPartitionId(PartitionId);
    event->Record.SetToken(Token);
    event->Record.SetAddBearerToToken(AddBearerToToken);
    if (ReadOffset) {
        event->Record.SetOffset(*ReadOffset);
    }

    Send(RowDispatcherActorId, event.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
}

void PqSession::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
}

void PqSession::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    SRC_LOG_D("TEvNodeDisconnected " << ev->Get()->NodeId);
}

void PqSession::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    SRC_LOG_D("TEvUndelivered, ev: " << ev->Get()->ToString());
}

std::unique_ptr<NActors::IActor> NewPqSession(
    const NPq::NProto::TDqPqTopicSource& settings,
    ui32 partitionId,
    TActorId rowDispatcherActorId,
    const TString& token,
    bool addBearerToToken,
    TMaybe<ui64> readOffset) {
    return std::unique_ptr<NActors::IActor>(new PqSession(settings, partitionId, rowDispatcherActorId, token, addBearerToToken, readOffset));
}

} // namespace NYql::NDq
