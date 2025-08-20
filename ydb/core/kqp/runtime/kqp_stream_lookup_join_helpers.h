#pragma once

#include <util/system/types.h>

namespace NKikimr::NKqp {

// maybe pack into a union?
struct TStreamLookupJoinRowCookie {
    ui64 RowSeqNo = 0;
    bool LastRow = false;
    bool FirstRow = false;

    ui64 Encode() const;
    static TStreamLookupJoinRowCookie Decode(ui64 cookie);
};

} // namespace NKikimr::NKqp