#pragma once

#include <set>
#include <map>
#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NKikimr::NColumnShard {
struct THistorgamBorders {
    static const std::map<i64, TString> BlobSizeBorders;

    static const std::map<i64, TString> BytesBorders;

    static const std::map<i64, TString> TimeBordersMicroseconds;

    static const std::set<i64> PortionRecordBorders;
};
}
