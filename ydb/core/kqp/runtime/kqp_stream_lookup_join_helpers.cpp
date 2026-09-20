#include "kqp_stream_lookup_join_helpers.h"

namespace NKikimr::NKqp {
namespace {

constexpr ui64 FirstRowMask = static_cast<ui64>(1) << 0;
constexpr ui64 LastRowMask = static_cast<ui64>(1) << 1;

ui64 ReservedBits(ui32 version) {
    return version == StreamLookupJoinCookieVersionLegacy
        ? StreamLookupJoinCookieV0ReservedBits
        : StreamLookupJoinCookieV1ReservedBits;
}

}

ui64 TStreamLookupJoinRowCookie::Encode(ui32 version) const {
    ui64 result = (RowSeqNo << ReservedBits(version));

    if (FirstRow) {
        result |= FirstRowMask;
    }

    if (LastRow) {
        result |= LastRowMask;
    }

    return result;
}

TStreamLookupJoinRowCookie TStreamLookupJoinRowCookie::Decode(ui64 encoded, ui32 version) {
    return TStreamLookupJoinRowCookie{
        .RowSeqNo = (encoded >> ReservedBits(version)),
        .LastRow = bool(encoded & LastRowMask),
        .FirstRow = bool(encoded & FirstRowMask)
    };
}


}
