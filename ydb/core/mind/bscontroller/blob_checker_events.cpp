#include "blob_checker_events.h"

namespace NKikimr {
namespace NBsController {

TEvBlobCheckerFinishQuantum::TEvBlobCheckerFinishQuantum(TGroupId groupId, EBlobCheckerWorkerQuantumStatus quantumStatus,
        TLogoBlobID maxCheckedBlob, ui32 unknownDataStatusCount, ui32 unknownPlacementStatusCount,
        std::vector<TLogoBlobID>&& blobsWithDataIssues, ui32 placementIssuesCount)
    : GroupId(groupId)
    , QuantumStatus(quantumStatus)
    , MaxCheckedBlob(maxCheckedBlob)
    , UnknownDataStatusCount(unknownDataStatusCount)
    , UnknownPlacementStatusCount(unknownPlacementStatusCount)
    , BlobsWithDataIssues(std::forward<std::vector<TLogoBlobID>>(blobsWithDataIssues))
    , PlacementIssuesCount(placementIssuesCount)
{}

TString TEvBlobCheckerFinishQuantum::ToString() const {
    TStringStream str;
    str << "TEvBlobCheckerFinishQuantum {";
    str << " GroupId# " << GroupId.GetRawId();
    str << " QuantumStatus# " << BlobCheckerWorkerQuantumStatusToString(QuantumStatus);
    str << " MaxCheckedBlob# " << MaxCheckedBlob.ToString();
    str << " UnknownDataStatusCount# " << UnknownDataStatusCount;
    str << " UnknownPlacementStatusCount# " << UnknownPlacementStatusCount;
    str << " BlobsWithDataIssues# " << BlobsWithDataIssues.size();
    str << " PlacementIssuesCount# " << PlacementIssuesCount;
    str << " }";

    return str.Str();
}

TEvBlobCheckerUpdateGroupStatus::TEvBlobCheckerUpdateGroupStatus(
        TGroupId groupId, TString serializedState, bool finishScan)
    : GroupId(groupId)
    , SerializedState(serializedState)
    , FinishScan(finishScan)
{}

TEvBlobCheckerUpdateGroupSet::TEvBlobCheckerUpdateGroupSet(
        std::unordered_map<TGroupId, TString>&& newGroups,
        std::vector<TGroupId>&& deletedGroups)
    : NewGroups(std::forward<std::unordered_map<TGroupId, TString>>(newGroups))
    , DeletedGroups(std::forward<std::vector<TGroupId>>(deletedGroups))
{}

TEvBlobCheckerPlanCheck::TEvBlobCheckerPlanCheck(TGroupId groupId)
    : GroupId(groupId)
{}

TEvBlobCheckerDecision::TEvBlobCheckerDecision(TGroupId groupId,
        NKikimrProto::EReplyStatus status)
    : GroupId(groupId)
    , Status(status)
{}

TEvBlobCheckerUpdateSettings::TEvBlobCheckerUpdateSettings(TDuration periodicity)
    : Periodicity(periodicity)
{}

TString TEvBlobCheckerUpdateSettings::ToString() const {
    TStringStream str;
    str << "TEvBlobCheckerUpdateSettings{ ";
    str << " Periodicity# " << Periodicity;
    str << " }";
    return str.Str();
}

} // namespace NBsController
} // namespace NKikimr
