#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <util/stream/str.h>

#include "blobstorage_pdisk_internal_interface.h"

namespace NKikimr {
namespace NPDisk {

TEvWhiteboardReportResult::~TEvWhiteboardReportResult() = default;

TString TEvWhiteboardReportResult::ToString(const TEvWhiteboardReportResult &record) {
    TStringStream str;
    str << "{";
    if (record.PDiskState) {
        str << "PDiskState# " << record.PDiskState->Record;
    }
    for (const auto& p : record.VDiskStateVect) {
        str << " VDiskState# " << std::get<1>(p);
    }
    if (record.DiskMetrics) {
        str << " DiskMetrics# " << record.DiskMetrics->Record;
    }
    str << "}";
    return str.Str();
}

} // NPDisk
} // NKikimr

