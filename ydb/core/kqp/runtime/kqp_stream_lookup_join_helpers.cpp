#include "kqp_stream_lookup_join_helpers.h"

namespace NKikimr::NKqp {
namespace {

constexpr ui64 FirstRowMask = static_cast<ui64>(1) << 0;
constexpr ui64 LastRowMask = static_cast<ui64>(1) << 1;
constexpr ui64 ReservedSpace = 20;
}

ui64 TStreamLookupJoinRowCookie::Encode() const {
    ui64 result = (RowSeqNo << ReservedSpace);

    if (FirstRow) {
        result |= FirstRowMask;
    }

    if (LastRow) {
        result |= LastRowMask;
    }

    return result;
}

TStreamLookupJoinRowCookie TStreamLookupJoinRowCookie::Decode(ui64 encoded) {
    return TStreamLookupJoinRowCookie{
        .RowSeqNo = (encoded >> ReservedSpace),
        .LastRow = bool(encoded & LastRowMask),
        .FirstRow = bool(encoded & FirstRowMask)
    };
}


}