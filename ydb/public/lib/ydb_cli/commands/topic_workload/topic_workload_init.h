#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TCommandWorkloadTopicInit: public TWorkloadCommand {
        public:
            TCommandWorkloadTopicInit();
            virtual void Config(TConfig& config) override;
            virtual void Parse(TConfig& config) override;
            virtual int Run(TConfig& config) override;

        private:
            TString TopicName;

            ui32 PartitionCount;
            ui32 ConsumerCount;
        };
    }
}
