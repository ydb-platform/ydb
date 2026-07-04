#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

namespace NKikimr::NPQ {

enum class ETopicSqsCountersBackend {
    Sqs,
    Ymq,
};

struct TTopicSqsActionCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr Success;
    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr Infly;
    ::NMonitoring::THistogramPtr Duration;
    ::NMonitoring::THistogramPtr WorkingDuration;
};

struct TTopicYmqActionCounters {
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr Success;
    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::THistogramPtr Duration;
};

struct TTopicTransactionCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr CompileQueryCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionsCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionsInfly;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionRetryTimeouts;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionRetries;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionsFailed;
};

struct TTopicQueueLeaderCounters {
    ::NMonitoring::TDynamicCounters::TCounterPtr RequestsThrottled;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueMasterStartProblems;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueLeaderStartProblems;

    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesPurged;
    ::NMonitoring::THistogramPtr MessageReceiveAttempts;
    ::NMonitoring::THistogramPtr ClientMessageProcessing_Duration;
    ::NMonitoring::THistogramPtr MessageReside_Duration;

    ::NMonitoring::TDynamicCounters::TCounterPtr DeleteMessage_Count;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReceiveMessage_EmptyCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReceiveMessage_Count;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReceiveMessage_BytesRead;

    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesMovedToDLQ;

    ::NMonitoring::TDynamicCounters::TCounterPtr SendMessage_DeduplicationCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr SendMessage_Count;
    ::NMonitoring::TDynamicCounters::TCounterPtr SendMessage_BytesWritten;

    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr InflyMessagesCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr OldestMessageAgeSeconds;

    TTopicTransactionCounters TransactionCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReceiveMessage_KeysInvalidated;
    ::NMonitoring::THistogramPtr ReceiveMessageImmediate_Duration;

    THashMap<TString, TTopicSqsActionCounters> SqsActionCounters;
    THashMap<TString, TTopicYmqActionCounters> YmqActionCounters;
};

class TTopicSqsMetricsHandler {
public:
    static bool IsApplicable(const NKikimrPQ::TPQTabletConfig& tabletConfig);

    TTopicSqsMetricsHandler(const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx);
    void Update(const TTabletLabeledCountersBase& mlpConsumerMetrics);

private:
    ETopicSqsCountersBackend Backend_;
    TTopicQueueLeaderCounters Counters_;
};

}
