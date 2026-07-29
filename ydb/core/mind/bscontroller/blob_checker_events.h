#pragma once

#include "blob_checker.h"
#include "defs.h"

namespace NKikimr {
namespace NBsController {

struct TEvBlobCheckerFinishQuantum : public TEventLocal<TEvBlobCheckerFinishQuantum,
        TEvBlobStorage::EvBlobCheckerFinishQuantum> {
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
        TEvBlobStorage::EvBlobCheckerUpdateGroupStatus> {
public:
    TEvBlobCheckerUpdateGroupStatus(TGroupId groupId, TString serializedState, bool finishScan);

public:
    TGroupId GroupId;
    TString SerializedState;
    bool FinishScan;
};

struct TEvBlobCheckerUpdateGroupSet : TEventLocal<TEvBlobCheckerUpdateGroupSet,
        TEvBlobStorage::EvBlobCheckerUpdateGroupSet> {
public:
    TEvBlobCheckerUpdateGroupSet(std::unordered_map<TGroupId, TString>&& newGroups,
            std::vector<TGroupId>&& deletedGroups);

public:
    std::unordered_map<TGroupId, TString> NewGroups;
    std::vector<TGroupId> DeletedGroups;
};

struct TEvBlobCheckerPlanCheck : TEventLocal<TEvBlobCheckerPlanCheck,
        TEvBlobStorage::EvBlobCheckerPlanCheck> {
public:
    TEvBlobCheckerPlanCheck(TGroupId groupId);

public:
    TGroupId GroupId;
};

struct TEvBlobCheckerDecision : TEventLocal<TEvBlobCheckerDecision,
        TEvBlobStorage::EvBlobCheckerDecision> {
public:
    TEvBlobCheckerDecision(TGroupId groupId, NKikimrProto::EReplyStatus status);

public:
    TGroupId GroupId;
    NKikimrProto::EReplyStatus Status;
};

struct TEvBlobCheckerUpdateSettings : TEventLocal<TEvBlobCheckerUpdateSettings,
        TEvBlobStorage::EvBlobCheckerUpdateSettings> {

public:
    TEvBlobCheckerUpdateSettings(TDuration periodicity);
    TString ToString() const;

public:
    TDuration Periodicity;
};

} // namespace NBsController
} // namespace NKikimr
