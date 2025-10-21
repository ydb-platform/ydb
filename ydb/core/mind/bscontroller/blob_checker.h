#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blob_scanner.pb.h>

namespace NKikimr {

enum EBlobCheckerResultStatusFlags : ui64 {
    DataIssues      = 0b0001,
    PlacementIssues = 0b0010,
};

enum EBlobCheckerWorkerQuantumStatus : ui32 {
    IntermediateOk = 0,
    Error,
    FinishOk,
};

TString BlobCheckerWorkerQuantumStatusToString(EBlobCheckerWorkerQuantumStatus value);

struct TGroupIdWithSerializedState {
    TGroupId GroupId;
    TString Data;
};

struct TEvBlobCheckerFinishQuantum : public TEventLocal<TEvBlobCheckerFinishQuantum, TEvPrivate::EvBlobCheckerFinishQuantum> {
public:
    TEvBlobCheckerFinishQuantum(TGroupId groupId, EBlobCheckerWorkerQuantumStatus quantumStatus,
            TLogoBlobID maxCheckedBlob, ui32 unknownDataStatusCount,
            ui32 unknownPlacementStatusCount, std::vector<TLogoBlobID>&& blobsWithDataIssues,
            ui32 placementIssuesCount);

    TString ToString() const;

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
        TEvPrivate::EvControllerBlobCheckerUpdateGroupStatus> {
public:
    TEvControllerBlobCheckerUpdateGroupStatus(TGroupId groupId, TString serializedState, bool finishScan);

public:
    TGroupId GroupId;
    TString SerializedState;
    bool FinishScan;
};

struct TEvBlobCheckerUpdateGroupSet : TEventLocal<TEvBlobCheckerUpdateGroupSet,
        TEvPrivate::EvBlobCheckerUpdateGroupSet> {
public:
    TEvBlobCheckerUpdateGroupSet(std::vector<TGroupIdWithSerializedState>&& newGroups,
            std::vector<TGroupId>&& deletedGroups);

public:
    std::vector<TGroupIdWithSerializedState> NewGroups;
    std::vector<TGroupId> DeletedGroups;
};

struct TEvBlobCheckerPlanScan : TEventLocal<TEvBlobCheckerPlanScan,
        TEvPrivate::EvBlobCheckerPlanScan> {
public:
    TEvBlobCheckerPlanScan(TGroupId groupId);

public:
    TGroupId GroupId;
};

struct TEvBlobCheckerScanDecision : TEventLocal<TEvBlobCheckerScanDecision,
        TEvPrivate::EvBlobCheckerScanDecision> {
public:
    TEvBlobCheckerScanDecision(TGroupId groupId, NKikimrProto::EReplyStatus status);

public:
    TGroupId GroupId;
    NKikimrProto::EReplyStatus Status;
};

struct TEvUpdateBlobCheckerSettings : TEventLocal<TEvUpdateBlobCheckerSettings,
        TEvPrivate::EvUpdateBlobCheckerSettings> {

public:
    TEvUpdateBlobCheckerSettings(TDuration periodicity);

public:
    TDuration Periodicity;
};

struct TBlobCheckerGroupState {
public:
    TBlobCheckerGroupState(ui64 status, TMonotonic lastScan, TLogoBlobID maxChecked);
    static TBlobCheckerGroupState Deserialize(NKikimrBlobChecker::TBlobCheckerGroupState serializedState);
    static NKikimrBlobChecker::TBlobCheckerGroupState CreateEmptySerialized();

public:
    ui64 ShortStatus;
    TMonotonic LastScanFinishedTimestamp;
    TLogoBlobID MaxCheckedBlob;
};

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        std::vector<TGroupIdWithSerializedState>&& serializedGroups);

} // namespace NKikimr
