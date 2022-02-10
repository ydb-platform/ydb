#pragma once

#include <library/cpp/actors/core/events.h>

namespace NYql {
namespace NDq {

struct TDqEvents {
    enum EEventSpaceDq {
        ES_DQ_COMPUTE_KQP_COMPATIBLE = 4145, // TKikimrEvents::ES_KQP
        ES_DQ_COMPUTE = 4212 //TKikimrEvents::ES_DQ
    };

    enum EDqEvents {
        EvAbortExecution = EventSpaceBegin(ES_DQ_COMPUTE_KQP_COMPATIBLE) + 15
    };
};

struct TDqComputeEvents {
    enum EDqComputeEvents {
        Unused0 = EventSpaceBegin(TDqEvents::ES_DQ_COMPUTE_KQP_COMPATIBLE) + 200,
        EvState,
        EvResumeExecution,
        EvChannelData,
        ReservedKqp_EvScanData,
        ReservedKqp_EvScanDataAck,
        EvChannelsInfo,
        EvChannelDataAck,
        ReservedKqp_EvScanError,
        Unused1,
        EvRetryChannelData,
        EvRetryChannelDataAck,
        ReservedKqp_EvScanInitActor,
        ReservedKqp_EvRemoteScanData,
        ReservedKqp_EvRemoteScanDataAck,

        EvRun = EventSpaceBegin(TDqEvents::ES_DQ_COMPUTE),

        EvNewCheckpointCoordinator,
        EvInjectCheckpoint,
        EvSaveTaskState,
        EvSaveTaskStateResult,
        EvCommitState,
        EvStateCommitted,
        EvRestoreFromCheckpoint,
        EvRestoreFromCheckpointResult,
        EvGetTaskState,
        EvGetTaskStateResult,
        EvStateRequest,
        EvNewCheckpointCoordinatorAck, 

        // place all new events here

        EvEnd
    };

    static_assert(EvEnd < EventSpaceBegin((TDqEvents::ES_DQ_COMPUTE + 1)));
};

} // namespace NDq
} // namespace NYql
