#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

IFileLogWriterPtr CreateFileLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TFileLogWriterConfigPtr config,
    ILogWriterHost* host);

ILogWriterFactoryPtr GetFileLogWriterFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
