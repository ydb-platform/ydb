#pragma once

#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NKqp {

struct IStreamingQueryCounters : public TThrRefBase {

    virtual ~IStreamingQueryCounters() = default;

    virtual void Update(const TAggExecStat& stats) = 0;
};

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path);

} // namespace NKikimr::NKqp
