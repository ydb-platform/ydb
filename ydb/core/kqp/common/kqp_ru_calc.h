#pragma once

#include <util/system/types.h>
#include <util/datetime/base.h>

#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

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
