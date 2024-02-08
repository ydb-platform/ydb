#pragma once

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TJobId, TGuid);

extern const TJobId NullJobId;

YT_DEFINE_STRONG_TYPEDEF(TOperationId, TGuid);

extern const TOperationId NullOperationId;

////////////////////////////////////////////////////////////////////////////////

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedArray.
DEFINE_ENUM(EJobType,
    // Scheduler jobs
    ((Map)               (  1))
    ((PartitionMap)      (  2))
    ((SortedMerge)       (  3))
    ((OrderedMerge)      (  4))
    ((UnorderedMerge)    (  5))
    ((Partition)         (  6))
    ((SimpleSort)        (  7))
    ((FinalSort)         (  8))
    ((SortedReduce)      (  9))
    ((PartitionReduce)   ( 10))
    ((ReduceCombiner)    ( 11))
    ((RemoteCopy)        ( 12))
    ((IntermediateSort)  ( 13))
    ((OrderedMap)        ( 14))
    ((JoinReduce)        ( 15))
    ((Vanilla)           ( 16))
    ((ShallowMerge)      ( 17))
    ((SchedulerUnknown)  ( 98)) // Used by node to report aborted jobs for which spec request has failed.

    // Master jobs
    ((ReplicateChunk)    (100))
    ((RemoveChunk)       (101))
    ((RepairChunk)       (102))
    ((SealChunk)         (103))
    ((MergeChunks)       (104))
    ((AutotomizeChunk)   (105))
    ((ReincarnateChunk)  (106))
);

constexpr auto FirstSchedulerJobType = EJobType::Map;
constexpr auto LastSchedulerJobType = EJobType::SchedulerUnknown;

constexpr auto FirstMasterJobType = EJobType::ReplicateChunk;
constexpr auto LastMasterJobType = EJobType::ReincarnateChunk;

// NB: Please keep the range of values small as this type
// is used as a key of TEnumIndexedArray.
DEFINE_ENUM(EJobState,
    ((Waiting)    (0))
    ((Running)    (1))
    // COMPAT(pogorelov): Remove it after 23.1
    ((Aborting)   (2))
    // |Completed| is used as sentinel in NJobTrackerClient::HasJobFinished.
    ((Completed)  (3))
    ((Failed)     (4))
    ((Aborted)    (5))
    // This sentinel is only used in TJob::GetStatisticsSuffix.
    ((Lost)       (7))
    // Initial state of newly created job.
    ((None)       (8))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
