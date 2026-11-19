#include "erasure.h"

#include <ydb/core/base/logoblob.h>

#include <ydb/library/yverify_stream/yverify_stream.h>


namespace NKikimr {

bool TChecksum::IsRequired(const TLogoBlobID& id) noexcept {
    switch (static_cast<TErasureType::ECrcMode>(id.CrcMode())) {
        case TErasureType::CrcModeNone:
            return false;
        case TErasureType::CrcModeWholePart:
            return true;
        default:
            Y_FAIL("Unknown crcMode: %d", id.CrcMode());
    }
}

} // namespace NKikimr
