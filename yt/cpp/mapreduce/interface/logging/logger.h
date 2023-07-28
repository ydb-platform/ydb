#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/compat.h>
#include <util/system/src_location.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class ILogger
    : public TThrRefBase
{
public:
    enum ELevel
    {
        FATAL /* "fatal", "FATAL" */,
        // We don't have such level as `warning', but we support it for compatibility with other APIs.
        ERROR /* "error", "warning", "ERROR", "WARNING" */,
        INFO /* "info", "INFO" */,
        DEBUG /* "debug", "DEBUG" */
    };

    virtual void Log(ELevel level, const ::TSourceLocation& sourceLocation, const char* format, va_list args) = 0;
};

using ILoggerPtr = ::TIntrusivePtr<ILogger>;

void SetLogger(ILoggerPtr logger);
ILoggerPtr GetLogger();

ILoggerPtr CreateStdErrLogger(ILogger::ELevel cutLevel);
ILoggerPtr CreateFileLogger(ILogger::ELevel cutLevel, const TString& path, bool append = false);

/**
 * Create logger that writes to a file in a buffered manner.
 * It should result in fewer system calls (useful if you expect a lot of log messages),
 * but in case of a crash, you would lose some log messages that haven't been flushed yet.
 */
ILoggerPtr CreateBufferedFileLogger(ILogger::ELevel cutLevel, const TString& path, bool append = false);

} // namespace NYT
