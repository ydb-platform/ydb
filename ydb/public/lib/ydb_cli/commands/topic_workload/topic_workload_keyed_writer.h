#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"
#include "topic_workload_reader_transaction_support.h"
#include "topic_workload_writer.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/log.h>

#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

// Keyed writer uses the same parameters as the regular writer for now.
struct TTopicWorkloadKeyedWriterParams : public TTopicWorkloadWriterParams {
    size_t ProducerKeysCount = 0;
};

class TTopicWorkloadKeyedWriterProducer;
class TTopicWorkloadKeyedWriterWorker {
public:
    static const size_t GENERATED_MESSAGES_COUNT = 32;
    static void RetryableWriterLoop(const TTopicWorkloadKeyedWriterParams& params);
    static void WriterLoop(const TTopicWorkloadKeyedWriterParams& params, TInstant endTime);
    static std::vector<TString> GenerateMessages(size_t messageSize);

private:
    TTopicWorkloadKeyedWriterWorker(const TTopicWorkloadKeyedWriterParams& params);
    ~TTopicWorkloadKeyedWriterWorker();

    void Close();
    void CloseProducers();

    std::shared_ptr<TTopicWorkloadKeyedWriterProducer> CreateProducer();

    void WaitTillNextMessageExpectedCreateTimeAndContinuationToken(
        std::shared_ptr<TTopicWorkloadKeyedWriterProducer> producer);

    void Process(TInstant endTime);

    TInstant GetExpectedCurrMessageCreationTimestamp() const;
    TInstant GetCreateTimestampForNextMessage();

    void TryCommitTx(TInstant& commitTime);
    void TryCommitTableChanges();

    size_t InflightMessagesSize();
    bool InflightMessagesEmpty();

    TStringBuilder LogPrefix();

    TTopicWorkloadKeyedWriterParams Params;
    ui64 BytesWritten = 0;
    std::optional<TTransactionSupport> TxSupport;
    TInstant StartTimestamp;

    std::vector<std::shared_ptr<TTopicWorkloadKeyedWriterProducer>> Producers;
    ui64 PartitionToWriteId = 0;

    std::shared_ptr<std::atomic<bool>> Closed;
    std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;

    bool WaitForCommitTx = false;
    std::string SessionId;
};

} // namespace NYdb::NConsoleClient
