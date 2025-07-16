#include "logger_init.h"

#include <yql/essentials/utils/log/log.h>

#include <atomic>

namespace NYql {
namespace NPureCalc {

namespace {
    std::atomic_bool Initialized;
}

    void InitLogging(const TLoggingOptions& options) {
        NLog::InitLogger(options.LogDestination);
        auto& logger = NLog::YqlLogger();
        logger.SetDefaultPriority(options.LogLevel);
        for (int i = 0; i < NLog::EComponentHelpers::ToInt(NLog::EComponent::MaxValue); ++i) {
            logger.SetComponentLevel((NLog::EComponent) i, (NLog::ELevel) options.LogLevel);
        }
        Initialized = true;
    }

    void EnsureLoggingInitialized() {
        if (Initialized.load()) {
            return;
        }
        InitLogging(TLoggingOptions());
    }

}
}
