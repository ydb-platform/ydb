#include "blob_checker.h"
#include "blob_checker_events.h"
#include "impl.h"

namespace NKikimr {
namespace NBsController {

TBlobCheckerGroupStatus::TBlobCheckerGroupStatus(ui64 status, TMonotonic lastScan,
        TLogoBlobID maxChecked)
    : ShortStatus(status)
    , LastScanFinishedTimestamp(lastScan)
    , MaxCheckedBlob(maxChecked)
{}

TBlobCheckerGroupStatus TBlobCheckerGroupStatus::Deserialize(TString serializedStatus) {
    NKikimrBlobChecker::TBlobCheckerGroupStatus status;
    if (!status.ParseFromString(serializedStatus)) {
        Y_DEBUG_ABORT_S("Unable to parse TBlobCheckerGroupStatus from string# "
                << HexEncode(serializedStatus));
        return TBlobCheckerGroupStatus{};
    }
    ui64 shortStatus = status.GetShortStatus();
    TMonotonic timestamp = TMonotonic::FromValue(status.GetLastScanFinishedTimestamp());
    TLogoBlobID blob = LogoBlobIDFromLogoBlobID(status.GetMaxCheckedBlob());
    return TBlobCheckerGroupStatus(shortStatus, timestamp, blob);
}

TString TBlobCheckerGroupStatus::CreateInitialSerialized(TMonotonic now) {
    NKikimrBlobChecker::TBlobCheckerGroupStatus proto;
    proto.SetShortStatus(0);
    proto.SetLastScanFinishedTimestamp(now.GetValue());
    LogoBlobIDFromLogoBlobID(TLogoBlobID(0, 0, 0), proto.MutableMaxCheckedBlob());
    TString serialized;
    bool success = proto.SerializeToString(&serialized);
    Y_DEBUG_ABORT_UNLESS(success);
    return serialized;
}

TString TBlobCheckerGroupStatus::SerializeProto() const {
    NKikimrBlobChecker::TBlobCheckerGroupStatus proto;
    proto.SetShortStatus(ShortStatus);
    proto.SetLastScanFinishedTimestamp(LastScanFinishedTimestamp.GetValue());
    LogoBlobIDFromLogoBlobID(MaxCheckedBlob, proto.MutableMaxCheckedBlob());
    TString serialized;
    bool success = proto.SerializeToString(&serialized);
    Y_VERIFY_DEBUG_S(success, ToString());
    return serialized;
}

TString TBlobCheckerGroupStatus::ToString() const {
    TStringStream str;
    str << "TBlobCheckerGroupStatus {"
        << " ShortStatus# " << ShortStatus
        << " LastScanFinishedTimestamp# " << LastScanFinishedTimestamp
        << " MaxCheckedBlob# " << MaxCheckedBlob.ToString()
        << " }";
    return str.Str();
}

TString BlobCheckerWorkerQuantumStatusToString(EBlobCheckerWorkerQuantumStatus value) {
    switch (value) {
    case EBlobCheckerWorkerQuantumStatus::IntermediateOk:
        return "IntermediateOk";
    case EBlobCheckerWorkerQuantumStatus::Error:
        return "Error";
    case EBlobCheckerWorkerQuantumStatus::FinishOk:
        return "FinishOk";
    }
}

} // namspace NBsController
} // namespace NKikimr
