#pragma once

#include "topic_operations_scenario.h"

namespace NYdb::NConsoleClient {

class TTopicReadScenario : public TTopicOperationsScenario {
    int DoRun(const TClientCommand::TConfig& config) override;
};

}
