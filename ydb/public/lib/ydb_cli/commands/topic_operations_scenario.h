#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <util/datetime/base.h>
#include <util/generic/fwd.h>
#include <util/system/types.h>

#include <atomic>
#include <future>
#include <memory>
#include <thread>
#include <vector>

class TLogBackend;
class TLog;

namespace NYdb {

class TDriver;

}

namespace NYdb::NTable {

class TSession;
class TTableClient;

}

namespace NYdb::NConsoleClient {

class TTopicWorkloadStatsCollector;

class TTopicOperationsScenario {
public:
    TTopicOperationsScenario();

    int Run(const TClientCommand::TConfig& config);

    void EnsurePercentileIsValid() const;
    void EnsureWarmupSecIsValid() const;

    TDuration TotalSec;
    TDuration WindowSec;
    TDuration WarmupSec;
    bool Quiet;
    bool PrintTimestamp;
    double Percentile;
    TString TopicName;
    ui32 TopicPartitionCount = 1;
    ui32 ProducerThreadCount;
    ui32 ConsumerThreadCount;
    ui32 ConsumerCount;
    TString ConsumerPrefix;
    size_t MessageSize;
    size_t MessageRate;
    size_t ByteRate;
    ui32 Codec;
    TString TableName;
    ui32 TablePartitionCount = 1;

protected:
    void CreateTopic(const TString& database,
                     const TString& topic,
                     ui32 partitionCount,
                     ui32 consumerCount);
    void DropTopic(const TString& database,
                   const TString& topic);

    void DropTable(const TString& database, const TString& table);

    void ExecSchemeQuery(const TString& query);

    void StartConsumerThreads(std::vector<std::future<void>>& threads,
                              const TString& database);
    void StartProducerThreads(std::vector<std::future<void>>& threads,
                              ui32 partitionCount,
                              ui32 partitionSeed,
                              const std::vector<TString>& generatedMessages);
    void JoinThreads(const std::vector<std::future<void>>& threads);

    bool AnyErrors() const;
    bool AnyIncomingMessages() const;
    bool AnyOutgoingMessages() const;

    std::unique_ptr<TDriver> Driver;
    std::shared_ptr<TLog> Log;
    std::shared_ptr<std::atomic_bool> ErrorFlag;
    std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;

private:
    virtual int DoRun(const TClientCommand::TConfig& config) = 0;

    void EnsureTopicNotExist(const TString& topic);
    void CreateTopic(const TString& topic,
                     ui32 partitionCount,
                     ui32 consumerCount);

    static NTable::TSession GetSession(NTable::TTableClient& client);

    static THolder<TLogBackend> MakeLogBackend(TClientCommand::TConfig::EVerbosityLevel level);

    void InitLog(const TClientCommand::TConfig& config);
    void InitDriver(const TClientCommand::TConfig& config);
    void InitStatsCollector();
};

}
