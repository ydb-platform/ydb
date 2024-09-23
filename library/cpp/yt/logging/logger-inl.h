#ifndef LOGGER_INL_H_
#error "Direct inclusion of this file is not allowed, include logger.h"
// For the sake of sane code completion.
#include "logger.h"
#endif
#undef LOGGER_INL_H_

#include <library/cpp/yt/yson_string/convert.h>
#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

inline bool TLogger::IsAnchorUpToDate(const TLoggingAnchor& position) const
{
    return
        !Category_ ||
        position.CurrentVersion == Category_->ActualVersion->load(std::memory_order::relaxed);
}

template <class... TArgs>
void TLogger::AddTag(const char* format, TArgs&&... args)
{
    AddRawTag(Format(TRuntimeFormat{format}, std::forward<TArgs>(args)...));
}

template <class TType>
void TLogger::AddStructuredTag(TStringBuf key, TType value)
{
    StructuredTags_.emplace_back(key, NYson::ConvertToYsonString(value));
}

template <class... TArgs>
TLogger TLogger::WithTag(const char* format, TArgs&&... args) const
{
    auto result = *this;
    result.AddTag(format, std::forward<TArgs>(args)...);
    return result;
}

template <class TType>
TLogger TLogger::WithStructuredTag(TStringBuf key, TType value) const
{
    auto result = *this;
    result.AddStructuredTag(key, value);
    return result;
}

Y_FORCE_INLINE bool TLogger::IsLevelEnabled(ELogLevel level) const
{
    // This is the first check which is intended to be inlined next to
    // logging invocation point. Check below is almost zero-cost due
    // to branch prediction (which requires inlining for proper work).
    if (level < MinLevel_) {
        return false;
    }

    // Next check is heavier and requires full log manager definition which
    // is undesirable in -inl.h header file. This is why we extract it
    // to a separate method which is implemented in cpp file.
    return IsLevelEnabledHeavy(level);
}

Y_FORCE_INLINE const TLogger& TLogger::operator()() const
{
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TMessageStringBuilderContext
{
    TSharedMutableRef Chunk;
};

struct TMessageBufferTag
{ };

class TMessageStringBuilder
    : public TStringBuilderBase
{
public:
    TSharedRef Flush();

    // For testing only.
    static void DisablePerThreadCache();

protected:
    void DoReset() override;
    void DoReserve(size_t newLength) override;

private:
    TSharedMutableRef Buffer_;

    static constexpr size_t ChunkSize = 128_KB - 64;
};

inline bool HasMessageTags(
    const TLoggingContext& loggingContext,
    const TLogger& logger)
{
    if (logger.GetTag()) {
        return true;
    }
    if (loggingContext.TraceLoggingTag) {
        return true;
    }
    return false;
}

inline void AppendMessageTags(
    TStringBuilderBase* builder,
    const TLoggingContext& loggingContext,
    const TLogger& logger)
{
    bool printComma = false;
    if (const auto& loggerTag = logger.GetTag()) {
        builder->AppendString(loggerTag);
        printComma = true;
    }
    if (auto traceLoggingTag = loggingContext.TraceLoggingTag) {
        if (printComma) {
            builder->AppendString(TStringBuf(", "));
        }
        builder->AppendString(traceLoggingTag);
    }
}

inline void AppendLogMessage(
    TStringBuilderBase* builder,
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TRef message)
{
    if (HasMessageTags(loggingContext, logger)) {
        if (message.Size() >= 1 && message[message.Size() - 1] == ')') {
            builder->AppendString(TStringBuf(message.Begin(), message.Size() - 1));
            builder->AppendString(TStringBuf(", "));
        } else {
            builder->AppendString(TStringBuf(message.Begin(), message.Size()));
            builder->AppendString(TStringBuf(" ("));
        }
        AppendMessageTags(builder, loggingContext, logger);
        builder->AppendChar(')');
    } else {
        builder->AppendString(TStringBuf(message.Begin(), message.Size()));
    }
}

template <class... TArgs>
void AppendLogMessageWithFormat(
    TStringBuilderBase* builder,
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TStringBuf format,
    TArgs&&... args)
{
    if (HasMessageTags(loggingContext, logger)) {
        if (format.size() >= 2 && format[format.size() - 1] == ')') {
            builder->AppendFormat(format.substr(0, format.size() - 1), std::forward<TArgs>(args)...);
            builder->AppendString(TStringBuf(", "));
        } else {
            builder->AppendFormat(format, std::forward<TArgs>(args)...);
            builder->AppendString(TStringBuf(" ("));
        }
        AppendMessageTags(builder, loggingContext, logger);
        builder->AppendChar(')');
    } else {
        builder->AppendFormat(format, std::forward<TArgs>(args)...);
    }
}

struct TLogMessage
{
    TSharedRef MessageRef;
    TStringBuf Anchor;
};

template <class T>
concept CLiteralLogFormat =
    requires (T& t) {
        [] (const char*) { } (t);
    };

template <class... TArgs>
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TFormatString<TArgs...> format,
    TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, loggingContext, logger, format.Get(), std::forward<TArgs>(args)...);
    return {builder.Flush(), format.Get()};
}

template <CFormattable T>
    requires (!CLiteralLogFormat<std::remove_cvref_t<T>>)
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    const T& obj)
{
    TMessageStringBuilder builder;
    FormatValue(&builder, obj, TStringBuf("v"));
    if (HasMessageTags(loggingContext, logger)) {
        builder.AppendString(TStringBuf(" ("));
        AppendMessageTags(&builder, loggingContext, logger);
        builder.AppendChar(')');
    }

    if constexpr (std::same_as<TStringBuf, std::remove_cvref_t<T>>) {
        // NB(arkady-e1ppa): This is the overload where TStringBuf
        // falls as well as zero-argument format strings.
        // Formerly (before static analysis) there was a special overload
        // which guaranteed that Anchor is set to the value of said TStringBuf
        // object. Now we have overload for TFormatString<> which fordids
        // us having overload for TStringBuf (both have implicit ctors from
        // string literals) thus we have to accommodate TStringBuf specifics
        // in this if constexpr part.
        return {builder.Flush(), obj};
    } else {
        return {builder.Flush(), TStringBuf()};
    }
}

inline TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TFormatString<> format)
{
    return BuildLogMessage(
        loggingContext,
        logger,
        format.Get());
}

inline TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TRuntimeFormat format)
{
    return BuildLogMessage(
        loggingContext,
        logger,
        format.Get());
}

inline TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TSharedRef&& message)
{
    if (HasMessageTags(loggingContext, logger)) {
        TMessageStringBuilder builder;
        AppendLogMessage(&builder, loggingContext, logger, message);
        return {builder.Flush(), TStringBuf()};
    } else {
        return {std::move(message), TStringBuf()};
    }
}

inline TLogEvent CreateLogEvent(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    ELogLevel level)
{
    TLogEvent event;
    event.Instant = loggingContext.Instant;
    event.Category = logger.GetCategory();
    event.Essential = logger.IsEssential();
    event.Level = level;
    event.ThreadId = loggingContext.ThreadId;
    event.ThreadName = loggingContext.ThreadName;
    event.FiberId = loggingContext.FiberId;
    event.TraceId = loggingContext.TraceId;
    event.RequestId = loggingContext.RequestId;
    return event;
}

void OnCriticalLogEvent(
    const TLogger& logger,
    const TLogEvent& event);

inline void LogEventImpl(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    ELogLevel level,
    ::TSourceLocation sourceLocation,
    TLoggingAnchor* anchor,
    TSharedRef message)
{
    auto event = CreateLogEvent(loggingContext, logger, level);
    event.MessageKind = ELogMessageKind::Unstructured;
    event.MessageRef = std::move(message);
    event.Family = ELogFamily::PlainText;
    event.SourceFile = sourceLocation.File;
    event.SourceLine = sourceLocation.Line;
    event.Anchor = anchor;
    logger.Write(std::move(event));
    if (Y_UNLIKELY(event.Level >= ELogLevel::Alert)) {
        OnCriticalLogEvent(logger, event);
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
