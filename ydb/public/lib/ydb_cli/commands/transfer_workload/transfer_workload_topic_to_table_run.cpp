#include "transfer_workload_topic_to_table_run.h"
#include "transfer_workload_defines.h"

#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_describe.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_params.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_reader.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_stats_collector.h>
#include <ydb/public/lib/ydb_cli/commands/topic_workload/topic_workload_writer.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

#include <future>
#include <thread>

using namespace NYdb::NConsoleClient;

TCommandWorkloadTransferTopicToTableRun::TCommandWorkloadTransferTopicToTableRun() :
    TWorkloadCommand("run", {}, "Run workload")
{
}

void TCommandWorkloadTransferTopicToTableRun::Config(TConfig& config)
{
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('s', "seconds", "Seconds to run workload.")
        .DefaultValue(60)
        .StoreResult(&Scenario.TotalSec);
    config.Opts->AddLongOption('w', "window", "Output window duration in seconds.")
        .DefaultValue(1)
        .StoreResult(&Scenario.WindowSec);
    config.Opts->AddLongOption('q', "quiet", "Quiet mode. Doesn't print statistics each second.")
        .StoreTrue(&Scenario.Quiet);
    config.Opts->AddLongOption("print-timestamp", "Print timestamp each second with statistics.")
        .StoreTrue(&Scenario.PrintTimestamp);
    config.Opts->AddLongOption("percentile", "Percentile for output statistics.")
        .DefaultValue(50)
        .StoreResult(&Scenario.Percentile);
    config.Opts->AddLongOption("warmup", "Warm-up time in seconds.")
        .DefaultValue(5)
        .StoreResult(&Scenario.WarmupSec);
    config.Opts->AddLongOption("topic", "Topic name.")
        .DefaultValue(NWorkloadTransfer::TOPIC)
        .StoreResult(&Scenario.TopicName);
    config.Opts->AddLongOption("consumer-prefix", "Use consumers with names '<consumer-prefix>-0' ... '<consumer-prefix>-<n-1>' where n is set in the '--consumers' option.")
        .DefaultValue(CONSUMER_PREFIX)
        .StoreResult(&Scenario.ConsumerPrefix);
    config.Opts->AddLongOption("table", "Table name.")
        .DefaultValue(NWorkloadTransfer::TABLE)
        .StoreResult(&Scenario.TableName);

    config.Opts->AddLongOption('p', "producer-threads", "Number of producer threads.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ProducerThreadCount);
    config.Opts->AddLongOption('t', "consumer-threads", "Number of consumer threads.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerThreadCount);
    config.Opts->AddLongOption('c', "consumers", "Number of consumers in a topic.")
        .DefaultValue(1)
        .StoreResult(&Scenario.ConsumerCount);
    config.Opts->AddLongOption('m', "message-size", "Message size.")
        .DefaultValue(10_KB)
        .StoreMappedResult(&Scenario.MessageSizeBytes, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("message-rate", "Total message rate for all producer threads (messages per second). Exclusive with --byte-rate.")
        .DefaultValue(0)
        .StoreResult(&Scenario.MessagesPerSec);
    config.Opts->AddLongOption("byte-rate", "Total message rate for all producer threads (bytes per second). Exclusive with --message-rate.")
        .DefaultValue(0)
        .StoreMappedResult(&Scenario.BytesPerSec, &TCommandWorkloadTopicParams::StrToBytes);
    config.Opts->AddLongOption("codec", PrepareAllowedCodecsDescription("Client-side compression algorithm. When read, data will be uncompressed transparently with a codec used on write", InitAllowedCodecs()))
        .Optional()
        .DefaultValue((TStringBuilder() << NTopic::ECodec::RAW))
        .StoreMappedResult(&Scenario.Codec, &TCommandWorkloadTopicParams::StrToCodec);

    config.Opts->MutuallyExclusive("message-rate", "byte-rate");

    config.Opts->AddLongOption("commit-period", "DEPRECATED: use tx-commit-intervall-ms instead. Waiting time between commit in seconds.")
        .DefaultValue(10)
        .Hidden()
        .StoreResult(&Scenario.CommitPeriodSeconds);
    config.Opts->AddLongOption("tx-commit-interval", "Interval of transaction commit in milliseconds."
                                                            " Both tx-commit-messages and tx-commit-interval can trigger transaction commit.")
        .Optional()
        .StoreResult(&Scenario.TxCommitIntervalMs);

    config.Opts->MutuallyExclusive("commit-period", "tx-commit-interval");

    config.Opts->AddLongOption("commit-messages", "DEPRECATED. Use --tx-commit-messages instead. Number of messages per transaction")
        .DefaultValue(1'000'000)
        .Hidden()
        .StoreResult(&Scenario.CommitMessages);
    config.Opts->AddLongOption("tx-commit-messages", "Number of messages to commit transaction. "
                                                            " Both tx-commit-messages and tx-commit-interval can trigger transaction commit.")
        .DefaultValue(1'000'000)
        .StoreResult(&Scenario.CommitMessages);

    config.Opts->MutuallyExclusive("commit-messages", "tx-commit-messages");

    config.Opts->AddLongOption("only-topic-in-tx", "Use only topic in transaction")
        .DefaultValue(false)
        .StoreTrue(&Scenario.OnlyTopicInTx);
    config.Opts->AddLongOption("only-table-in-tx", "Use only table in transaction")
        .DefaultValue(false)
        .StoreTrue(&Scenario.OnlyTableInTx);

    config.Opts->MutuallyExclusive("only-topic-in-tx", "only-table-in-tx");

    config.IsNetworkIntensive = true;
}

void TCommandWorkloadTransferTopicToTableRun::Parse(TConfig& config)
{
    TClientCommand::Parse(config);

    Scenario.EnsurePercentileIsValid();
    Scenario.EnsureWarmupSecIsValid();
    Scenario.EnsureRatesIsValid();
}

int TCommandWorkloadTransferTopicToTableRun::Run(TConfig& config)
{
    Scenario.UseTransactions = true;

    return Scenario.Run(config);
}
