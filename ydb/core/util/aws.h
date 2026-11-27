#pragma once

#include <util/generic/string.h>

namespace NKikimr {

struct TAwsClientConfig {
    struct TLogConfig {
        int LogLevel = 0; // Aws::Utils::Logging::LogLevel, 0 is disabled
        TString FilenamePrefix; // Aws::Utils::Logging::DefaultLogSystem::FilenamePrefix
    };

    TLogConfig LogConfig;
};

void InitAwsAPI(const TAwsClientConfig& config = {});
void ShutdownAwsAPI(const TAwsClientConfig& config = {});

}
