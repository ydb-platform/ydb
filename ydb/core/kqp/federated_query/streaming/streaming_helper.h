#pragma once

#include "pq_topic_resolver.h"

#include <ydb/core/kqp/common/kqp_execution_stats.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NKqp {

class TKqpPhyTxHolder;

TVector<TPqTopicResolverSource> CollectPqSources(const TVector<std::shared_ptr<const TKqpPhyTxHolder>>& transactions, const TString& database);

struct IStreamingQueryCounters: public TThrRefBase {
    virtual ~IStreamingQueryCounters() = default;

    virtual void Update(const TAggExecStat& stats) = 0;
};

TIntrusivePtr<IStreamingQueryCounters> MakeStreamingQueryCounters(const ::NMonitoring::TDynamicCounterPtr& counters, const TString& path);

}   // namespace NKikimr::NKqp
