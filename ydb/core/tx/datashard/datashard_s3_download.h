#pragma once

#include <ydb/core/protos/checksum.pb.h>

#include <util/generic/maybe.h>

namespace NKikimr {
namespace NDataShard {

struct TS3Download {
    TMaybe<TString> DataETag;
    ui64 ProcessedBytes = 0;
    ui64 WrittenBytes = 0;
    ui64 WrittenRows = 0;
    NKikimrBackup::TChecksumState ChecksumState;

    void Out(IOutputStream& out) const {
        out << "{"
            << " DataETag: " << DataETag
            << " ProcessedBytes: " << ProcessedBytes
            << " WrittenBytes: " << WrittenBytes
            << " WrittenRows: " << WrittenRows
            << " ChecksumState: " << ChecksumState.ShortDebugString()
        << " }";
    }
};

} // namespace NDataShard
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NDataShard::TS3Download, out, value) {
    value.Out(out);
}
