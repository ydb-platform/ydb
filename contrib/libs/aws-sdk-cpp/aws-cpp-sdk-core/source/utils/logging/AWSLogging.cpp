/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/stl/AWSStack.h>

#include <memory>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;

namespace {

// AWS clients owned by other globals can be destroyed after this translation
// unit's statics (static destruction order across translation units is
// unspecified) and still log through GetLogSystem() while unwinding. The flag
// is trivially destructible, so it stays readable until the very end of the
// process; once ~LoggingSet has cleared it, the accessors below behave as
// if no logger is installed instead of touching the destroyed shared_ptrs.
bool LoggingSetAlive = true;

struct LoggingSet {
    std::shared_ptr<LogSystemInterface> AWSLogSystem;
    std::shared_ptr<LogSystemInterface> OldLogger;

    ~LoggingSet() {
        LoggingSetAlive = false;
    }
};

LoggingSet LogSet;

} // anonymous namespace

namespace Aws
{
namespace Utils
{
namespace Logging {

void InitializeAWSLogging(const std::shared_ptr<LogSystemInterface> &logSystem) {
    if (LoggingSetAlive) {
        LogSet.AWSLogSystem = logSystem;
    }
}

void ShutdownAWSLogging(void) {
    InitializeAWSLogging(nullptr);
}

LogSystemInterface *GetLogSystem() {
    return LoggingSetAlive ? LogSet.AWSLogSystem.get() : nullptr;
}

void PushLogger(const std::shared_ptr<LogSystemInterface> &logSystem)
{
    if (!LoggingSetAlive) {
        return;
    }
    LogSet.OldLogger = LogSet.AWSLogSystem;
    LogSet.AWSLogSystem = logSystem;
}

void PopLogger()
{
    if (!LoggingSetAlive) {
        return;
    }
    LogSet.AWSLogSystem = LogSet.OldLogger;
    LogSet.OldLogger = nullptr;
}

} // namespace Logging
} // namespace Utils
} // namespace Aws
