#include "topic_workload.h"

#include "topic_workload_defines.h"
#include "topic_workload_clean.h"
#include "topic_workload_init.h"
#include "topic_workload_run_read.h"
#include "topic_workload_run_write.h"
#include "topic_workload_run_full.h"

using namespace NYdb::NConsoleClient;

TCommandWorkloadTopic::TCommandWorkloadTopic()
    : TClientCommandTree("topic", {}, "YDB topic workload")
{
    AddCommand(std::make_unique<TCommandWorkloadTopicInit>());
    AddCommand(std::make_unique<TCommandWorkloadTopicClean>());
    AddCommand(std::make_unique<TCommandWorkloadTopicRun>());
}

TCommandWorkloadTopicRun::TCommandWorkloadTopicRun()
    : TClientCommandTree("run", {}, "Run YDB topic workload")
{
    AddCommand(std::make_unique<TCommandWorkloadTopicRunRead>());
    AddCommand(std::make_unique<TCommandWorkloadTopicRunWrite>());
    AddCommand(std::make_unique<TCommandWorkloadTopicRunFull>());
}
