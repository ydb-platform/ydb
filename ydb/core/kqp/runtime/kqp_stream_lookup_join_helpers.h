#pragma once

#include <util/system/types.h>

namespace NKikimr::NKqp {

// Cookie format versions for the stream lookup join per-row RowSeqNo wire value.
//
// v0 (legacy) packs RowSeqNo into the high 44 bits and reserves the low 20 bits
// for flags. Because the seqno allocator builds rowSeqNo as (taskId << 40) | counter,
// a full 64-bit value, the extra `<< 20` in Encode overflows and truncates the
// taskId prefix for any taskId >= 16 — distinct rows from different distributed
// tasks then collide on the same RowSeqNo. See StreamLookupJoin_RowSeqNoCollision_Repro.
//
// v1 fixes the bit budget: RowSeqNo occupies the high 60 bits (shifted by only 4),
// so taskId (20 bits) + counter (40 bits) round-trip losslessly.
//
// The format version is chosen per query from a config flag and passed explicitly
// to every producer and consumer of the cookie; it is never inferred from the
// encoded value.
inline constexpr ui32 StreamLookupJoinCookieVersionLegacy = 0;
inline constexpr ui32 StreamLookupJoinCookieVersionV1 = 1;

inline constexpr ui32 StreamLookupJoinCookieV0ReservedBits = 20;
inline constexpr ui32 StreamLookupJoinCookieV1ReservedBits = 4;

// maybe pack into a union?
struct TStreamLookupJoinRowCookie {
    ui64 RowSeqNo = 0;
    bool LastRow = false;
    bool FirstRow = false;

    ui64 Encode(ui32 version) const;
    static TStreamLookupJoinRowCookie Decode(ui64 cookie, ui32 version);
};

} // namespace NKikimr::NKqp
