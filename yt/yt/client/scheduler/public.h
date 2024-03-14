#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::TJobId;
using NJobTrackerClient::TOperationId;

DEFINE_ENUM(EOperationType,
    (Map)
    (Merge)
    (Erase)
    (Sort)
    (Reduce)
    (MapReduce)
    (RemoteCopy)
    (JoinReduce)
    (Vanilla)
);

DEFINE_ENUM(EOperationState,
    (None)
    (Starting)
    (Orphaned)
    (WaitingForAgent)
    (Initializing)
    (Preparing)
    (Materializing)
    (ReviveInitializing)
    (Reviving)
    (RevivingJobs)
    (Pending)
    (Running)
    (Completing)
    (Completed)
    (Aborting)
    (Aborted)
    (Failing)
    (Failed)
);

YT_DEFINE_ERROR_ENUM(
    ((NoSuchOperation)                        (200))
    ((InvalidOperationState)                  (201))
    ((TooManyOperations)                      (202))
    ((NoSuchAllocation)                       (203))
    ((AgentRevoked)                           (204))
    ((OperationFailedOnJobRestart)            (210))
    ((OperationFailedWithInconsistentLocking) (211))
    ((OperationControllerCrashed)             (212))
    ((TestingError)                           (213))
    ((PoolTreesAreUnspecified)                (214))
    ((MaxFailedJobsLimitExceeded)             (215))
    ((OperationFailedToPrepare)               (216))
    ((WatcherHandlerFailed)                   (217))
    ((MasterDisconnected)                     (218))
    ((NoSuchJobShell)                         (219))
);

DEFINE_ENUM(EUnavailableChunkAction,
    (Fail)
    (Skip)
    (Wait)
);

DEFINE_ENUM(ESchemaInferenceMode,
    (Auto)
    (FromInput)
    (FromOutput)
);

// NB(eshcherbin): This enum must be synchronized at schedulers ans CAs.
// If you change it, you must bump the controller agent tracker service protocol version!
DEFINE_ENUM(EAbortReason,
    ((None)                            (  0))
    ((Scheduler)                       (  1))
    ((FailedChunks)                    (  2))
    ((ResourceOverdraft)               (  3))
    ((Other)                           (  4))
    ((Preemption)                      (  5))
    ((UserRequest)                     (  6))
    ((NodeOffline)                     (  7))
    ((WaitingTimeout)                  (  8))
    ((AccountLimitExceeded)            (  9))
    ((GetSpecFailed)                   ( 10))
    ((Unknown)                         ( 11))
    ((RevivalConfirmationTimeout)      ( 12))
    ((IntermediateChunkLimitExceeded)  ( 13))
    ((NodeBanned)                      ( 14))
    ((SpeculativeRunWon)               ( 15))
    ((SpeculativeRunLost)              ( 16))
    ((ChunkMappingInvalidated)         ( 17))
    ((NodeWithDisabledJobs)            ( 18))
    ((NodeSchedulingSegmentChanged)    ( 19))
    ((NodeFairShareTreeChanged)        ( 20))
    ((JobOnUnexpectedNode)             ( 21))
    ((ShallowMergeFailed)              ( 22))
    ((InconsistentJobState)            ( 23))
    // COMPAT(pogorelov)
    ((JobStatisticsWaitTimeout)        ( 24))
    ((OperationFailed)                 ( 25))
    ((JobRevivalDisabled)              ( 26))
    ((BannedInTentativeTree)           ( 27))
    ((DisappearedFromNode)             ( 28))
    ((Unconfirmed)                     ( 29))
    ((OperationSuspended)              ( 30))
    ((ProbingRunWon)                   ( 31))
    ((ProbingRunLost)                  ( 32))
    ((ProbingToUnsuccessfulJob)        ( 33))
    ((JobProxyFailed)                  ( 34))
    ((InterruptionTimeout)             ( 35))
    ((NodeResourceOvercommit)          ( 36))
    ((ProbingCompetitorResultLost)     ( 37))
    ((SpeculativeCompetitorResultLost) ( 38))
    ((JobTreatmentFailed)              ( 39))
    ((JobTreatmentResultLost)          ( 40))
    ((JobTreatmentToUnsuccessfulJob)   ( 41))
    ((JobTreatmentRunLost)             ( 42))
    ((JobTreatmentRunWon)              ( 43))
    ((OperationCompleted)              ( 44))
    ((OperationAborted)                ( 45))
    ((OperationFinished)               ( 46))
    ((JobMemoryThrashing)              ( 47))
    ((InterruptionUnsupported)         ( 48))
    ((Abandoned)                       ( 49))
    // TODO(ignat): is it actually a scheduling type of abortion?
    ((JobSettlementTimedOut)           ( 50))
    ((NonexistentPoolTree)             ( 51))
    ((WrongSchedulingSegmentModule)    ( 52))
    ((UnresolvedNodeId)                ( 53))
    ((RootVolumePreparationFailed)     ( 54))
    ((SchedulingFirst)                 (100))
    ((SchedulingTimeout)               (101))
    ((SchedulingResourceOvercommit)    (102))
    ((SchedulingOperationSuspended)    (103))
    ((SchedulingJobSpecThrottling)     (104))
    ((SchedulingOther)                 (105))
    ((SchedulingOperationDisabled)     (106))
    ((SchedulingOperationIsNotAlive)   (107))
    ((SchedulingLast)                  (199))
);

DEFINE_ENUM(EInterruptReason,
    ((None)               (0))
    ((Preemption)         (1))
    ((UserRequest)        (2))
    ((JobSplit)           (3))
    ((Unknown)            (4))
    ((JobsDisabledOnNode) (5))
);

DEFINE_ENUM(EAutoMergeMode,
    (Disabled)
    (Relaxed)
    (Economy)
    (Manual)
);

DECLARE_REFCOUNTED_CLASS(TOperationCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
