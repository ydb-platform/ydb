#include "common.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NDataSharing {

TString TCommonSession::DebugString() const {
    return TStringBuilder() << "{id=" << SessionId << ";snapshot=" << SnapshotBarrier.DebugString() << ";}";
}

}