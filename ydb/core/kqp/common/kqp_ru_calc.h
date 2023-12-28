#pragma once

#include <util/system/types.h>
#include <util/datetime/base.h>

namespace NKqpProto {
    class TKqpStatsQuery;
}

namespace NKikimr {
namespace NKqp {

struct TProgressStatEntry;

namespace NRuCalc {

ui64 CpuTimeToUnit(TDuration cpuTimeUs);
ui64 CalcRequestUnit(const NKqpProto::TKqpStatsQuery& stats);
ui64 CalcRequestUnit(const TProgressStatEntry& stats);

} // namespace NRuCalc
} // namespace NKqp
} // namespace NKikimr
