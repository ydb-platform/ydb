#include "datashard_s3_download.h"

#include <ydb/library/protobuf_printer/security_printer.h>

#include <util/stream/output.h>

namespace NKikimr {
namespace NDataShard {

void TS3Download::Out(IOutputStream& out) const {
    out << "{"
        << " DataETag: " << DataETag
        << " ProcessedBytes: " << ProcessedBytes
        << " WrittenBytes: " << WrittenBytes
        << " WrittenRows: " << WrittenRows
        << " ChecksumState: " << ChecksumState.ShortDebugString()
        << " DownloadState: " << SecureDebugString(DownloadState) // Can hold secure encryption key
    << " }";
}

} // namespace NDataShard
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NDataShard::TS3Download, out, value) {
    value.Out(out);
}
