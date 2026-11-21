#pragma once

#include <util/system/types.h>
#include <util/datetime/base.h>

#include <ydb/core/protos/kqp_stats.pb.h>
#include "progress.h"

namespace NYql::NDqProto {
    class TDqTableStats;
}

namespace NKqpProto {
    class TKqpStatsQuery;
}

namespace NKikimr {
namespace NKqp {

struct TProgressStatEntry;

namespace NRuCalc {

ui64 CalcReadIORu(const TTableStat& stat);

class TIoReadStat: public TTableStat {
public:
    void Add(const NYql::NDqProto::TDqTableStats& tableAggr);
    ui64 CalcRu() const;
};

class TIoWriteStat: public TTableStat {
public:
    void Add(const NYql::NDqProto::TDqTableStats& tableAggr);
    ui64 CalcRu() const;
};

ui64 CpuTimeToUnit(TDuration cpuTimeUs);
ui64 CalcRequestUnit(const TProgressStatEntry& stats);

} // namespace NRuCalc
} // namespace NKqp
} // namespace NKikimr
