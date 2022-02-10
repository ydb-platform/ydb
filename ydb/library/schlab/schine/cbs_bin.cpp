#include "cbs_bin.h"

namespace NKikimr {
namespace NSchLab {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCbsBin

bool TCbsBin::TLogReader::IsEof() const {
    return (Pos >= Log.LogPos);
}

TCbsBin::ELog TCbsBin::TLogReader::Kind() const {
    ui8 kind = static_cast<ui8>(Log.Log.data()[Pos]);
    return static_cast<TCbsBin::ELog>(kind);
}

ui64 TCbsBin::TLogReader::FixedSize() const {
    ui8 kind = static_cast<ui8>(Log.Log.data()[Pos]);
    return static_cast<ui64>(KindData[kind].FixedSize);
}

ui64 TCbsBin::TLogReader::GetFixedUi64() const {
    const ui64 *p = static_cast<const ui64*>(static_cast<const void*>(Log.Log.data() + Pos + 1));
    return *p;
}

const char* TCbsBin::TLogReader::GetVarSizeData() const {
    ui8 kind = static_cast<ui8>(Log.Log.data()[Pos]);
    if (!KindData[kind].IsVarSize) {
        return nullptr;
    }
    return (Log.Log.data() + Pos + 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCbsBin

const TCbsBin::TKind TCbsBin::KindData[256] = {
    {8, false, 1}, // LogTsBegin,
    {0, false, 0}, // LogKeyframe,
    {8, false, 0}, // LogEpochtime,
    {8, false, 1}, // LogCbsIdxBegin,
    {0, true,  0}, // LogCbsName,
    {8, false, 0}, // LogCbsMaxBudget,
    {8, false, 0}, // LogCbsDeadline,
    {8, false, 0}, // LogCbsState,
    {8, false, 0}, // LogCbsCurBudget,
    {8, false, 1}, // LogCbsReqIdBegin,
    {8, false, 0}, // LogCbsReqCost,
    {8, false, 0}, // LogCbsReqState,
    {8, false, 0}, // LogCbsReqSeqno,
    {0, false, -1}, // LogCbsReqEnd,
    {0, false, 1}, // LogCbsEnd,
    {0, false, 1}, // LogHgrubBegin,
    {8, false, 0}, // LogHgrubUact,
    {0, false, -1}, // LogHgrubEnd,
    {0, false, -1}, // LogEnd
};

bool TCbsBin::AppendDiff(const TCbs &prev, const TCbs &cur) {
    Y_UNUSED(prev);
    Y_UNUSED(cur);
    // TODO(cthulhu): Don't check here, actually write the diff and rollback the LogPos in case of a failure.
    return false;
}

bool TCbsBin::AppendKeyframe(const TCbs &cur) {
    Y_UNUSED(cur);
    //if (Log.size() < LogPos + cur.LogPos) {
    //    return false;
    //}
    //memcpy(const_cast<char*>(Log.data()), cur.data(), cur.LogPos);
    //LogPos += cur.LogPos;
    return false;
}

} // NSchLab
} // NKikimr
