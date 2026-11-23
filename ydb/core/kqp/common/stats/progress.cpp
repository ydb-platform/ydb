#include "progress.h"

namespace NKikimr::NKqp {

TTableStat& TTableStat::operator +=(const TTableStat& rhs) {
    Rows += rhs.Rows;
    Bytes += rhs.Bytes;
    return *this;
}

TTableStat& TTableStat::operator -=(const TTableStat& rhs) {
    Rows -= rhs.Rows;
    Bytes -= rhs.Bytes;
    return *this;
}

TProgressStatEntry& TProgressStatEntry::operator +=(const TProgressStatEntry& rhs) {
    ComputeTime += rhs.ComputeTime;
    ReadIOStat += rhs.ReadIOStat;

    return *this;
}

TTableStat CalcSumTableReadStat(const TProgressStatEntry& entry) {
    return entry.ReadIOStat;
}

TDuration CalcCumComputeTime(const TProgressStatEntry& entry) {
    return entry.ComputeTime;
}

void TProgressStatEntry::Out(IOutputStream& o) const {
    o << "ComputeTime: " << ComputeTime << " ReadRows: " << ReadIOStat.Rows << " ReadBytes: " << ReadIOStat.Bytes;
}

} // namespace NKikimr::NKqp
