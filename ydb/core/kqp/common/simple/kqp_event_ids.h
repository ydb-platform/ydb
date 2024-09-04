#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>

namespace NKikimr {
namespace NKqp {

struct TKqpEvents {
    enum EKqpEvents {
        EvQueryRequest = EventSpaceBegin(TKikimrEvents::ES_KQP),
        EvQueryResponse,
        EvContinueProcess,
        EvQueryTimeout,
        EvIdleTimeout,
        EvCloseSessionRequest,
        EvProcessResponse,
        EvCreateSessionRequest,
        EvCreateSessionResponse,
        EvPingSessionRequest,
        EvCloseSessionResponse,
        EvPingSessionResponse,
        EvCompileRequest,
        EvCompileResponse,
        EvCompileInvalidateRequest,
        EvAbortExecution = NYql::NDq::TDqEvents::EDqEvents::EvAbortExecution,
        EvInitiateShutdownRequest,
        EvInitiateSessionShutdown,
        EvContinueShutdown,
        EvDataQueryStreamPart,
        EvDataQueryStreamPartAck,
        EvRecompileRequest,
        EvScriptRequest,
        EvScriptResponse,
        EvFetchScriptResultsResponse,
        EvKqpProxyPublishRequest,
        EvCancelScriptExecutionRequest,
        EvCancelScriptExecutionResponse,
        EvCancelQueryRequest,
        EvCancelQueryResponse,
        EvParseRequest,
        EvParseResponse,
        EvSplitResponse,
        EvListSessionsRequest,
        EvListSessionsResponse,
        EvListProxyNodesRequest,
        EvListProxyNodesResponse
    };

    static_assert (EvCompileInvalidateRequest + 1 == EvAbortExecution);
};


struct TKqpExecuterEvents {
    enum EKqpExecuterEvents {
        EvTxRequest = EventSpaceBegin(TKikimrEvents::ES_KQP) + 100,
        EvTxResponse,
        EvStreamData,
        EvStreamProfile,
        EvProgress,
        EvStreamDataAck,
        EvTableResolveStatus,
        EvShardsResolveStatus
    };
};

struct TKqpSnapshotEvents {
    enum EKqpSnapshotEvents {
        EvCreateSnapshotRequest = EventSpaceBegin(TKikimrEvents::ES_KQP) + 150,
        EvCreateSnapshotResponse,
        EvDiscardSnapshot
    };
};

struct TKqpComputeEvents {
    enum EKqpComputeEvents {
        Unused0 = NYql::NDq::TDqComputeEvents::EDqComputeEvents::Unused0,
        EvState = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvState,
        EvResumeExecution = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvResumeExecution,
        EvChannelData = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvChannelData,
        EvScanData,
        EvScanDataAck,
        EvChannelsInfo = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvChannelsInfo,
        EvChannelDataAck = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvChannelDataAck,
        EvScanError,
        EvKillScanTablet = NYql::NDq::TDqComputeEvents::EDqComputeEvents::Unused1,
        EvRetryChannelData = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvRetryChannelData,
        EvRetryChannelDataAck = NYql::NDq::TDqComputeEvents::EDqComputeEvents::EvRetryChannelDataAck,
        EvScanInitActor,
        EvRemoteScanData,
        EvRemoteScanDataAck,
    };

    static_assert(Unused0 == EventSpaceBegin(TKikimrEvents::ES_KQP) + 200);
    static_assert(EvState == Unused0 + 1);
    static_assert(EvResumeExecution == EvState + 1);
    static_assert(EvChannelData == EvResumeExecution + 1);
    static_assert(EvScanData == EvChannelData + 1);
    static_assert(EvScanDataAck == EvScanData + 1);
    static_assert(EvChannelsInfo == EvScanDataAck + 1);
    static_assert(EvChannelDataAck == EvChannelsInfo + 1);
    static_assert(EvScanError == EvChannelDataAck + 1);
    static_assert(EvKillScanTablet == EvScanError + 1);
    static_assert(EvRetryChannelData == EvKillScanTablet + 1);
    static_assert(EvRetryChannelDataAck == EvRetryChannelData + 1);
    static_assert(EvScanInitActor == EvRetryChannelDataAck + 1);
};

struct TKqpResourceManagerEvents {
    enum EKqpResourceManagerEvents {
        EvStartComputeTasks = EventSpaceBegin(TKikimrEvents::ES_KQP) + 300,
        EvStartComputeTasksFailure,
        EvStartedComputeTasks,
        EvFinishComputeTask,
        EvCancelComputeTasks,
        Unused0,
        EvStartDsComputeTasks,
        Unused3, // EvEstimateResourcesRequest,
        Unused4, // EvEstimateResourcesResponse,
        Unused1, // EvAllocateTaskResourcesRequest, extra resources allocation
        Unused2, // EvAllocateTaskResourcesResponse
    };
};

struct TKqpSpillingEvents {
    enum EKqpSpillingEvents {
        EvWrite = EventSpaceBegin(TKikimrEvents::ES_KQP) + 400,
        EvWriteResult,
        EvRead,
        EvReadResult,
        EvError,
    };
};

struct TKqpScriptExecutionEvents {
    enum EKqpScriptExecutionEvents {
        EvGetScriptExecutionOperation = EventSpaceBegin(TKikimrEvents::ES_KQP) + 500,
        EvGetScriptExecutionOperationResponse,
        EvListScriptExecutionOperations,
        EvListScriptExecutionOperationsResponse,
        EvScriptLeaseUpdateResponse,
        EvCancelScriptExecutionOperation,
        EvCancelScriptExecutionOperationResponse,
        EvScriptExecutionFinished,
        EvForgetScriptExecutionOperation,
        EvForgetScriptExecutionOperationResponse,
        EvSaveScriptResultMetaFinished,
        EvSaveScriptResultFinished,
        EvCheckAliveRequest,
        EvCheckAliveResponse,
        EvFetchScriptResultsResponse,
        EvSaveScriptExternalEffectRequest,
        EvSaveScriptExternalEffectResponse,
        EvScriptFinalizeRequest,
        EvScriptFinalizeResponse,
        EvSaveScriptFinalStatusResponse,
        EvGetScriptExecutionOperationQueryResponse,
        EvDescribeSecretsResponse,
        EvSaveScriptResultPartFinished,
        EvScriptExecutionsTableCreationFinished,
    };
};

struct TKqpResourceInfoExchangerEvents {
    enum EKqpResourceInfoExchangerEvents {
        EvPublishResource = EventSpaceBegin(TKikimrEvents::ES_KQP) + 600,
        EvSendResources,
    };
};

struct TKqpWorkloadServiceEvents {
    enum EKqpWorkloadServiceEvents {
        EvPlaceRequestIntoPool = EventSpaceBegin(TKikimrEvents::ES_KQP) + 700,
        EvContinueRequest,
        EvCleanupRequest,
        EvCleanupResponse,
        EvUpdatePoolInfo,
        EvUpdateDatabaseInfo,
        EvSubscribeOnPoolChanges,
    };
};

} // namespace NKqp
} // namespace NKikimr
