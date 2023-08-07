#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

ILogWriterPtr CreateStreamLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name,
    IOutputStream* stream);

ILogWriterPtr CreateStderrLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name);

ILogWriterFactoryPtr GetStderrLogWriterFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
