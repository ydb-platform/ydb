#include "kqp_ru_calc.h"

#include <ydb/core/protos/kqp_stats.pb.h>

#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

#include <ydb/core/kqp/common/kqp.h>

#include <util/generic/size_literals.h>

#include <cmath>

namespace NKikimr {
namespace NKqp {
namespace NRuCalc {

namespace {

ui64 CalcReadIORu(const TTableStat& stat) {
    constexpr ui64 bytesPerUnit = 4_KB;
    constexpr ui64 bytesPerUnitAdjust = bytesPerUnit - 1;

    auto bytes = (stat.Bytes + bytesPerUnitAdjust) / bytesPerUnit;
    return std::max(bytes, stat.Rows);
}

class TIoReadStat: public TTableStat {
public:
    void Add(const NYql::NDqProto::TDqTableStats& tableAggr) {
        Rows += tableAggr.GetReadRows();
        Bytes += tableAggr.GetReadBytes();
    }

    ui64 CalcRu() const {
        return CalcReadIORu(*this);
    }
};

class TIoWriteStat: public TTableStat {
public:
    void Add(const NYql::NDqProto::TDqTableStats& tableAggr) {
        Rows += tableAggr.GetWriteRows();
        Rows += tableAggr.GetEraseRows();
        Bytes += tableAggr.GetWriteBytes();
    }

    ui64 CalcRu() const {
        constexpr ui64 bytesPerUnit = 1_KB;
        constexpr ui64 bytesPerUnitAdjust = bytesPerUnit - 1;

        auto bytes = (Bytes + bytesPerUnitAdjust) / bytesPerUnit;
        return 2 * std::max(bytes, Rows);
    }
};

}

ui64 CpuTimeToUnit(TDuration cpuTime) {
    return std::floor(cpuTime.MicroSeconds() / 1500.0);
}

ui64 CalcRequestUnit(const NKqpProto::TKqpStatsQuery& stats) {
    TDuration totalCpuTime;
    TIoReadStat totalReadStat;
    TIoWriteStat totalWriteStat;

    for (const auto& exec : stats.GetExecutions()) {
        totalCpuTime += TDuration::MicroSeconds(exec.GetCpuTimeUs());

        for (auto& table : exec.GetTables()) {
            totalReadStat.Add(table);
        }
    }

    if (stats.HasCompilation()) {
        totalCpuTime += TDuration::MicroSeconds(stats.GetCompilation().GetCpuTimeUs());
    }

    totalCpuTime += TDuration::MicroSeconds(stats.GetWorkerCpuTimeUs());

    auto totalIoRu = totalReadStat.CalcRu() + totalWriteStat.CalcRu();

    return std::max(std::max(CpuTimeToUnit(totalCpuTime), totalIoRu), (ui64)1);
}

ui64 CalcRequestUnit(const TProgressStatEntry& stats) {
    auto ioRu = CalcReadIORu(stats.ReadIOStat);

    return std::max(std::max(CpuTimeToUnit(stats.ComputeTime), ioRu), (ui64)1);
}

} // namespace NRuCalc
} // namespace NKqp
} // namespace NKikimr
