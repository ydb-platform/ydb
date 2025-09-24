#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blob_scanner.pb.h>

namespace NKikimr {

enum EGroupScanResultStatusFlags : ui64 {
    DataIssues      = 0b0001,
    PlacementIssues = 0b0010,
};

enum EBlobCheckerWorkerQuantumStatus : ui32 {
    IntermediateOk = 0,
    Error,
    LastQuantumOk,
};

enum {
    EvFinishQuantum = EventSpaceBegin(TEvents::ES_PRIVATE),
};


struct TEvFinishQuantum : public TEventLocal<TEvFinishQuantum, EvFinishQuantum> {
public:
    TEvFinishQuantum(TGroupId groupId, EBlobCheckerWorkerQuantumStatus quantumStatus,
            TLogoBlobID maxCheckedBlob, ui32 unknownDataStatusCount,
            ui32 unknownPlacementStatusCount, std::vector<TLogoBlobID>&& blobsWithDataIssues,
            ui32 placementIssuesCount);

public:
    TGroupId GroupId;
    EBlobCheckerWorkerQuantumStatus QuantumStatus;
    TLogoBlobID MaxCheckedBlob;
    ui32 UnknownDataStatusCount;
    ui32 UnknownPlacementStatusCount;
    std::vector<TLogoBlobID> BlobsWithDataIssues;
    ui32 PlacementIssuesCount;
};


struct TEvControllerBlobCheckerUpdateGroupStatus : TEventLocal<TEvControllerBlobCheckerUpdateGroupStatus,
        EvControllerBlobCheckerUpdateGroupStatus> {
public:
    TEvControllerBlobCheckerUpdateGroupStatus(TGroupId groupId, TString serializedState)
        : GroupId(groupId)
        , SerializedState(serializedState)
    {}

public:
    TGroupId GroupId;
    TString SerializedState;
};


struct TEvControllerBlobCheckerUpdateGroupSet : TEventLocal<TEvControllerBlobCheckerUpdateGroupSet,
        EvControllerBlobCheckerUpdateGroupSet> {
public:
    TEvControllerBlobCheckerUpdateGroupStatus(std::unordered_map<TGroupId, TString>&& newGroups,
            std::vector<TGroupId>&& deletedGroups)
        : GroupId(groupId)
        , SerializedState(serializedState)
    {}

public:
    std::unordered_map<TGroupId, TString> NewGroups;
    std::vector<TGroupId> DeletedGroups;
};

struct TBlobCheckerGroupState {
public:
    TBlobCheckerGroupState(NKikimrBlobChecker::TBlobCheckerGroupState serializedState);

public:
    ui64 ShortStatus;
    TMonotonic LastScanFinishedTimestamp;
    TLogoBlobID MaxCheckedBlob;
    bool CheckPlanned;
};

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        TSerializedBlobCheckerGroupStates serializedStates);

} // namespace NKikimr
