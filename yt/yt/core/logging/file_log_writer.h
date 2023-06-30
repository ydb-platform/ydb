#pragma once

#include "private.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

IFileLogWriterPtr CreateFileLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name,
    TFileLogWriterConfigPtr config,
    ILogWriterHost* host);

ILogWriterFactoryPtr GetFileLogWriterFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
