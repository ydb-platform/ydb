#pragma once

#include "topic_workload_stats_collector.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

#include <library/cpp/logger/log.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicRunWrite: public TWorkloadCommand {
        public:
            TCommandWorkloadTopicRunWrite();
            virtual void Config(TConfig& config) override;
            virtual void Parse(TConfig& config) override;
            virtual int Run(TConfig& config) override;
        private:
            ui32 Seconds;
            ui32 Warmup;
            ui8 Percentile;

            size_t MessageRate;
            size_t ByteRate;
            size_t MessageSize;
            ui32 Codec;

            ui32 ProducerThreadCount;

            std::shared_ptr <TLog> Log;
            std::shared_ptr<std::atomic_bool> ErrorFlag;

            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
        };
    }
}
