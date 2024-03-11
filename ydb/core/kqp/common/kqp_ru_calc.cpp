#include "kqp_ru_calc.h"

#include <ydb/core/kqp/common/kqp.h>

#include <util/generic/size_literals.h>

#include <cmath>

namespace NKikimr {
namespace NKqp {
namespace NRuCalc {

ui64 CalcReadIORu(const TTableStat& stat) {
    constexpr ui64 bytesPerUnit = 4_KB;
    constexpr ui64 bytesPerUnitAdjust = bytesPerUnit - 1;

    auto bytes = (stat.Bytes + bytesPerUnitAdjust) / bytesPerUnit;
    return std::max(bytes, stat.Rows);
}

void TIoReadStat::Add(const NYql::NDqProto::TDqTableStats& tableAggr) {
    Rows += tableAggr.GetReadRows();
    Bytes += tableAggr.GetReadBytes();
}

ui64 TIoReadStat::CalcRu() const {
    return CalcReadIORu(*this);
}

void TIoWriteStat::Add(const NYql::NDqProto::TDqTableStats& tableAggr) {
    Rows += tableAggr.GetWriteRows();
    Rows += tableAggr.GetEraseRows();
    Bytes += tableAggr.GetWriteBytes();
}

ui64 TIoWriteStat::CalcRu() const {
    constexpr ui64 bytesPerUnit = 1_KB;
    constexpr ui64 bytesPerUnitAdjust = bytesPerUnit - 1;

    auto bytes = (Bytes + bytesPerUnitAdjust) / bytesPerUnit;
    return 2 * std::max(bytes, Rows);
}

ui64 CpuTimeToUnit(TDuration cpuTime) {
    return std::floor(cpuTime.MicroSeconds() / 1500.0);
}

ui64 CalcRequestUnit(const TProgressStatEntry& stats) {
    auto ioRu = CalcReadIORu(stats.ReadIOStat);

    return std::max(std::max(CpuTimeToUnit(stats.ComputeTime), ioRu), (ui64)1);
}

} // namespace NRuCalc
} // namespace NKqp
} // namespace NKikimr
