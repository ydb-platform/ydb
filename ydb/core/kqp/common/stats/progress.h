#pragma once

#include <util/datetime/base.h>
#include <util/stream/output.h>
#include <util/system/types.h>

namespace NKikimr::NKqp {

struct TTableStat {
    ui64 Rows = 0;
    ui64 Bytes = 0;

    TTableStat& operator+=(const TTableStat& rhs);
    TTableStat& operator-=(const TTableStat& rhs);
};

struct TProgressStatEntry {
    TDuration ComputeTime;
    TTableStat ReadIOStat;
    bool Defined = false;

    TProgressStatEntry& operator+=(const TProgressStatEntry& rhs);

    void Out(IOutputStream& o) const;
};

TTableStat CalcSumTableReadStat(const TProgressStatEntry& entry);
TDuration CalcCumComputeTime(const TProgressStatEntry& entry);

} // namespace NKikimr::NKqp

template<>
inline void Out<NKikimr::NKqp::TProgressStatEntry>(IOutputStream& o, const NKikimr::NKqp::TProgressStatEntry& x) {
    return x.Out(o);
}
