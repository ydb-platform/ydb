#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NPQ {

class TTopicSqsMetricsHandler {
public:
    static bool IsApplicable(const NKikimrPQ::TPQTabletConfig& tabletConfig);

    TTopicSqsMetricsHandler(const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx);
    void Update(const TTabletLabeledCountersBase& mlpConsumerMetrics);

private:
    enum class ECountersType {
        Sqs,
        Ymq,
    };

    void SetMessageCounts(ui64 storedCount, ui64 inflightCount);

    ECountersType CountersType_;
    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesCount_;
    ::NMonitoring::TDynamicCounters::TCounterPtr InflightMessagesCount_;
};

}
