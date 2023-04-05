#pragma once

#include <util/generic/string.h>

#define WRITE_LOG(log, priority, str)       \
    if (log->FiltrationLevel() >= priority) \
        log->Write(priority, str);

namespace NYdb::NConsoleClient {
    const TString PRODUCER_PREFIX = "workload-producer";
    const TString CONSUMER_PREFIX = "workload-consumer";
    const TString MESSAGE_GROUP_ID_PREFIX = "workload-message-group-id";
    const TString TOPIC = "workload-topic";
}