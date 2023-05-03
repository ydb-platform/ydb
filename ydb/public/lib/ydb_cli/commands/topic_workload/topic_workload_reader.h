#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"

#include <library/cpp/logger/log.h>
#include <util/system/types.h>
#include <util/string/type.h>
#include <util/generic/size_literals.h>

namespace NYdb {
    namespace NConsoleClient {
        struct TTopicWorkloadReaderParams {
            size_t Seconds;
            NYdb::TDriver* Driver;
            std::shared_ptr<TLog> Log;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
            std::shared_ptr<std::atomic_bool> ErrorFlag;
            std::shared_ptr<std::atomic_uint> StartedCount;

            ui32 ConsumerIdx;
        };

        class TTopicWorkloadReader {
        public:
            static void ReaderLoop(TTopicWorkloadReaderParams&& params);
        };
    }
}