#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DefaultStderrWriterName = "Stderr";
constexpr TStringBuf DefaultFileWriterName = "File";

constexpr ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
constexpr ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_TYPE(TFixedBufferFileOutput)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
