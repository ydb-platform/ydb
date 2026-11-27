#include "datashard_s3_download.h"

#include <ydb/core/base/row_version.h>
#include <ydb/library/protobuf_printer/security_printer.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NDataShard {

bool TS3Download::HasWrittenAtVersion(const TRowVersion& version) const {
    if (WrittenRows == 0) {
        return true;
    }

    const auto& progress = DownloadState.GetRestoreWriteProgress();
    return progress.HasVersion()
        && progress.HasWrittenRows()
        && TRowVersion::FromProto(progress.GetVersion()) == version
        && progress.GetWrittenRows() == WrittenRows;
}

void TS3Download::Out(IOutputStream& out) const {
    out << "{"
        << " DataETag: " << DataETag
        << " ProcessedBytes: " << ProcessedBytes
        << " WrittenBytes: " << WrittenBytes
        << " WrittenRows: " << WrittenRows
        << " ProcessedChecksumState: " << ProcessedChecksumState.ShortDebugString()
        << " DownloadState: " << SecureDebugString(DownloadState) // Can hold secure encryption key
    << " }";
}

} // namespace NDataShard
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TS3Download, out, value) {
    value.Out(out);
}
