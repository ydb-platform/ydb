#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blob_scanner.pb.h>

namespace NKikimr {

enum EBlobCheckerResultStatusFlags : ui64 {
    ScanFinished    = 0b0001,
    DataIssues      = 0b0010,
    PlacementIssues = 0b0100,
};

enum EBlobCheckerWorkerQuantumStatus : ui32 {
    IntermediateOk = 0,
    Error,
    FinishOk,
};

TString BlobCheckerWorkerQuantumStatusToString(EBlobCheckerWorkerQuantumStatus value);

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

struct TEvBlobCheckerUpdateGroupStatus : TEventLocal<TEvBlobCheckerUpdateGroupStatus,
        TEvPrivate::EvBlobCheckerUpdateGroupStatus> {
public:
    TEvBlobCheckerUpdateGroupStatus(TGroupId groupId, TString serializedState, bool finishScan);

public:
    TGroupId GroupId;
    TString SerializedState;
    bool FinishScan;
};

struct TEvBlobCheckerUpdateGroupSet : TEventLocal<TEvBlobCheckerUpdateGroupSet,
        TEvPrivate::EvBlobCheckerUpdateGroupSet> {
public:
    TEvBlobCheckerUpdateGroupSet(std::unordered_map<TGroupId, TString>&& newGroups,
            std::vector<TGroupId>&& deletedGroups);

public:
    std::unordered_map<TGroupId, TString> NewGroups;
    std::vector<TGroupId> DeletedGroups;
};

struct TEvBlobCheckerPlanCheck : TEventLocal<TEvBlobCheckerPlanCheck,
        TEvPrivate::EvBlobCheckerPlanCheck> {
public:
    TEvBlobCheckerPlanCheck(TGroupId groupId);

public:
    TGroupId GroupId;
};

struct TEvBlobCheckerDecision : TEventLocal<TEvBlobCheckerDecision,
        TEvPrivate::EvBlobCheckerDecision> {
public:
    TEvBlobCheckerDecision(TGroupId groupId, NKikimrProto::EReplyStatus status);

public:
    TGroupId GroupId;
    NKikimrProto::EReplyStatus Status;
};

struct TEvBlobCheckerUpdateSettings : TEventLocal<TEvBlobCheckerUpdateSettings,
        TEvPrivate::EvBlobCheckerUpdateSettings> {

public:
    TEvBlobCheckerUpdateSettings(TDuration periodicity);

public:
    TDuration Periodicity;
};

struct TBlobCheckerGroupStatus {
public:
    TBlobCheckerGroupStatus(ui64 status, TMonotonic lastScan, TLogoBlobID maxChecked);
    static TBlobCheckerGroupStatus Deserialize(NKikimrBlobChecker::TBlobCheckerGroupStatus serializedState);
    static TString CreateInitialSerialized();

public:
    ui64 ShortStatus;
    TMonotonic LastScanFinishedTimestamp;
    TLogoBlobID MaxCheckedBlob;
};

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        std::unordered_map<TGroupId, TString> serializedGroups,
        TDuration periodicity);

} // namespace NKikimr
