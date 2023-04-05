#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicClean: public TWorkloadCommand {
        public:
            TCommandWorkloadTopicClean();
            virtual void Config(TConfig& config) override;
            virtual void Parse(TConfig& config) override;
            virtual int Run(TConfig& config) override;
        };

        class TCommandWorkloadTopicRun: public TClientCommandTree {
        public:
            TCommandWorkloadTopicRun();

        private:
        };
    }
}
