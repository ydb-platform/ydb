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
class TParams;

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

    TString GetReadOnlyTableName() const;
    TString GetWriteOnlyTableName() const;

    TDuration TotalSec;
    TDuration WindowSec;
    TDuration WarmupSec;
    bool Quiet = false;
    bool PrintTimestamp = false;
    double Percentile = 99.0;
    TString TopicName;
    ui32 TopicPartitionCount = 1;
    bool TopicAutoscaling = false;
    ui32 TopicMaxPartitionCount = 100;
    ui32 StabilizationWindowSeconds = 15;
    ui32 UpUtilizationPercent = 90;
    ui32 DownUtilizationPercent = 30;
    ui32 ProducerThreadCount = 0;
    ui32 ConsumerThreadCount = 0;
    ui32 ConsumerCount = 0;
    bool Direct = false;
    TString ConsumerPrefix;
    size_t MessageSize;
    size_t MessageRate;
    size_t ByteRate;
    ui32 Codec;
    TString TableName;
    ui32 TablePartitionCount = 1;
    bool UseTransactions = false;
    size_t CommitPeriod = 10;
    size_t CommitMessages = 1'000'000;
    bool OnlyTopicInTx = false;
    bool OnlyTableInTx = false;
    bool UseTableSelect = true;
    bool ReadWithoutConsumer = false;

protected:
    void CreateTopic(const TString& database,
                     const TString& topic,
                     ui32 partitionCount,
                     ui32 consumerCount,
                     bool autoscaling = false,
                     ui32 maxPartitionCount = 100,
                     ui32 stabilizationWindowSeconds = 15,
                     ui32 upUtilizationPercent = 90,
                     ui32 downUtilizationPercent = 30);
    void DropTopic(const TString& database,
                   const TString& topic);

    void DropTable(const TString& database, const TString& table);

    void ExecSchemeQuery(const TString& query);
    void ExecDataQuery(const TString& query, const NYdb::TParams& params);

    void StartConsumerThreads(std::vector<std::future<void>>& threads,
                              const TString& database);
    void StartProducerThreads(std::vector<std::future<void>>& threads,
                              ui32 partitionCount,
                              ui32 partitionSeed,
                              const std::vector<TString>& generatedMessages,
                              const TString& database);
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
                     ui32 consumerCount,
                     bool autoscaling,
                     ui32 maxPartitionCount,
                     ui32 stabilizationWindowSeconds,
                     ui32 upUtilizationPercent,
                     ui32 downUtilizationPercent);

    static NTable::TSession GetSession(NTable::TTableClient& client);

    static THolder<TLogBackend> MakeLogBackend(TClientCommand::TConfig::EVerbosityLevel level);

    void InitLog(const TClientCommand::TConfig& config);
    void InitDriver(const TClientCommand::TConfig& config);
    void InitStatsCollector();
};

}
