#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

ILogWriterPtr CreateStreamLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    std::string name,
    TLogWriterConfigPtr config,
    IOutputStream* stream);

ILogWriterPtr CreateStderrLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    std::string name,
    TStderrLogWriterConfigPtr config);

ILogWriterFactoryPtr GetStderrLogWriterFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
