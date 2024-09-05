#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! A special category for logs produced by the logging subsystem itself.
//! Logs in this category are written by a separate stderr writer.
constexpr TStringBuf SystemLoggingCategoryName = "Logging";

DEFINE_ENUM(ELogFormat,
    (PlainText)
    (Json)
    // Legacy alias for JSON.
    (Structured)
    (Yson)
);

DEFINE_ENUM(ECompressionMethod,
    (Gzip)
    (Zstd)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLogManagerConfig)
DECLARE_REFCOUNTED_CLASS(TLogManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TFormatterConfig)
DECLARE_REFCOUNTED_CLASS(TLogWriterConfig)
DECLARE_REFCOUNTED_CLASS(TRuleConfig)
DECLARE_REFCOUNTED_CLASS(TFileLogWriterConfig)
DECLARE_REFCOUNTED_CLASS(TRotationPolicyConfig)
DECLARE_REFCOUNTED_CLASS(TStderrLogWriterConfig)

struct ILogFormatter;
struct ISystemLogEventProvider;
struct ILogWriterHost;
DECLARE_REFCOUNTED_STRUCT(ILogWriterFactory)
DECLARE_REFCOUNTED_STRUCT(ILogWriter)
DECLARE_REFCOUNTED_STRUCT(IFileLogWriter)
DECLARE_REFCOUNTED_STRUCT(IStreamLogOutput)
DECLARE_REFCOUNTED_STRUCT(ILogCompressionCodec)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
