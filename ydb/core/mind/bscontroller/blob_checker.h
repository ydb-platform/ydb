#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blob_checker.pb.h>

namespace NKikimr {
namespace NBsController {

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

// In-memory representation of status stored in BSC
struct TBlobCheckerGroupStatus {
public:
    TBlobCheckerGroupStatus() = default;
    TBlobCheckerGroupStatus(ui64 status, TMonotonic lastScan, TLogoBlobID maxChecked);
    static TBlobCheckerGroupStatus Deserialize(TString serializedStatus);
    static TString CreateInitialSerialized(TMonotonic now);
    TString SerializeProto() const;
    TString ToString() const;

public:
    ui64 ShortStatus;
    TMonotonic LastScanFinishedTimestamp;
    TLogoBlobID MaxCheckedBlob;
};

} // namespace NBsController
} // namespace NKikimr
