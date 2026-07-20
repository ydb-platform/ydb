#ifndef LOGGER_INL_H_
#error "Direct inclusion of this file is not allowed, include logger.h"
// For the sake of sane code completion.
#include "logger.h"
#endif

#include "tagged_payload.h"

#include <library/cpp/yt/yson_string/convert.h>
#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

inline bool TLogger::IsAnchorUpToDate(const TLoggingAnchor& anchor) const
{
    return
        !Category_ ||
        anchor.CurrentVersion == Category_->ActualVersion->load(std::memory_order::relaxed);
}

template <class... TArgs>
void TLogger::AddTag(TFormatString<TArgs...> format, TArgs&&... args)
{
    AddRawTag(Format(format, std::forward<TArgs>(args)...));
}

template <class TType>
void TLogger::AddStructuredTag(TStringBuf key, TType value)
{
    auto* state = GetMutableCoWState();
    state->StructuredTags.emplace_back(key, NYson::ConvertToYsonString(value));
}

template <class... TArgs>
TLogger TLogger::WithTag(TFormatString<TArgs...> format, TArgs&&... args) const &
{
    auto result = *this;
    result.AddTag(format, std::forward<TArgs>(args)...);
    return result;
}

template <class... TArgs>
TLogger TLogger::WithTag(TFormatString<TArgs...> format, TArgs&&... args) &&
{
    AddTag(format, std::forward<TArgs>(args)...);
    return std::move(*this);
}

template <class TType>
TLogger TLogger::WithStructuredTag(TStringBuf key, TType value) const &
{
    auto result = *this;
    result.AddStructuredTag(key, value);
    return result;
}

template <class TType>
TLogger TLogger::WithStructuredTag(TStringBuf key, TType value) &&
{
    AddStructuredTag(key, value);
    return std::move(*this);
}

Y_FORCE_INLINE ELogLevel TLogger::GetEffectiveLoggingLevel(ELogLevel level, const TLoggingAnchor& anchor)
{
    // Check if anchor is suppressed.
    if (anchor.Suppressed.load(std::memory_order::relaxed)) {
        return ELogLevel::Minimum;
    }

    // Compute the actual level taking anchor override into account.
    return anchor.LevelOverride.load(std::memory_order::relaxed).value_or(level);
}

Y_FORCE_INLINE bool TLogger::IsLevelEnabled(ELogLevel level) const
{
    if (!Category_ || level < MinLevel_) {
        return false;
    }

    [[unlikely]] if (
        Category_->CurrentVersion.load(std::memory_order::relaxed) !=
        Category_->ActualVersion->load(std::memory_order::relaxed))
    {
        UpdateCategory();
    }

    if (level < Category_->MinPlainTextLevel) {
        return false;
    }

    if (level < GetThreadMinLogLevel()) {
        return false;
    }

    return true;
}

Y_FORCE_INLINE const TLogger& TLogger::operator()() const
{
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline bool HasMessageTags(
    const TLoggingContext& loggingContext,
    const TLogger& logger)
{
    if (!logger.GetTag().empty()) {
        return true;
    }
    if (!loggingContext.TraceLoggingTag.empty()) {
        return true;
    }
    if (!GetThreadMessageTag().empty()) {
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
    if (const auto& loggerTag = logger.GetTag(); !loggerTag.empty()) {
        builder->AppendString(loggerTag);
        printComma = true;
    }
    if (const auto& traceLoggingTag = loggingContext.TraceLoggingTag; !traceLoggingTag.empty()) {
        if (printComma) {
            builder->AppendString(", "_sb);
        }
        builder->AppendString(traceLoggingTag);
        printComma = true;
    }
    if (const auto& threadMessageTag = GetThreadMessageTag(); !threadMessageTag.empty()) {
        if (printComma) {
            builder->AppendString(", "_sb);
        }
        builder->AppendString(threadMessageTag);
        printComma = true;
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
            builder->AppendString(", "_sb);
        } else {
            builder->AppendString(TStringBuf(message.Begin(), message.Size()));
            builder->AppendString(" ("_sb);
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
            builder->AppendFormat(TRuntimeFormat{format.substr(0, format.size() - 1)}, std::forward<TArgs>(args)...);
            builder->AppendString(", "_sb);
        } else {
            builder->AppendFormat(TRuntimeFormat{format}, std::forward<TArgs>(args)...);
            builder->AppendString(" ("_sb);
        }
        AppendMessageTags(builder, loggingContext, logger);
        builder->AppendChar(')');
    } else {
        builder->AppendFormat(TRuntimeFormat{format}, std::forward<TArgs>(args)...);
    }
}

struct TLogMessage
{
    TTaggedLogEventPayload Payload;
    TStringBuf Anchor;
};

template <class... TArgs>
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    TFormatString<TArgs...> format,
    TArgs&&... args)
{
    TTaggedPayloadWriter writer;
    AppendLogMessageWithFormat(writer.BeginMessage(), loggingContext, logger, format.Get(), std::forward<TArgs>(args)...);
    writer.EndMessage();
    return {writer.Finish(), format.Get()};
}

template <CFormattable T>
    requires (!CStringLiteral<std::remove_cvref_t<T>>)
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    const T& obj)
{
    TTaggedPayloadWriter writer;
    auto* builder = writer.BeginMessage();
    FormatValue(builder, obj, "v"_sb);
    if (HasMessageTags(loggingContext, logger)) {
        builder->AppendString(" ("_sb);
        AppendMessageTags(builder, loggingContext, logger);
        builder->AppendChar(')');
    }
    writer.EndMessage();

    if constexpr (std::same_as<TStringBuf, std::remove_cvref_t<T>>) {
        // NB(arkady-e1ppa): This is the overload where TStringBuf
        // falls as well as zero-argument format strings.
        // Formerly (before static analysis) there was a special overload
        // which guaranteed that Anchor is set to the value of said TStringBuf
        // object. Now we have overload for TFormatString<> which fordids
        // us having overload for TStringBuf (both have implicit ctors from
        // string literals) thus we have to accommodate TStringBuf specifics
        // in this if constexpr part.
        return {writer.Finish(), obj};
    } else {
        return {writer.Finish(), TStringBuf()};
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
    TTaggedPayloadWriter writer;
    AppendLogMessage(writer.BeginMessage(), loggingContext, logger, message);
    writer.EndMessage();
    return {writer.Finish(), TStringBuf()};
}

inline TLogEvent CreateLogEvent(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    ELogLevel level)
{
    TLogEvent event;
    event.Category = logger.GetCategory();
    event.Level = level;
    event.Essential = logger.IsEssential();
    event.Instant = loggingContext.Instant;
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
    TTaggedLogEventPayload payload)
{
    auto event = TLogEvent{
        .Category = logger.GetCategory(),
        .Level = level,
        .Family = ELogFamily::PlainText,
        .Essential = logger.IsEssential(),
        .Payload = std::move(payload),
        .Instant = loggingContext.Instant,
        .ThreadId = loggingContext.ThreadId,
        .ThreadName = loggingContext.ThreadName,
        .FiberId = loggingContext.FiberId,
        .TraceId = loggingContext.TraceId,
        .RequestId = loggingContext.RequestId,
        .SourceFile = sourceLocation.File,
        .SourceLine = sourceLocation.Line,
        .Anchor = anchor,
    };
    if (Y_UNLIKELY(event.Level >= ELogLevel::Alert)) {
        logger.Write(TLogEvent(event));
        OnCriticalLogEvent(logger, event);
    } else {
        logger.Write(std::move(event));
    }
}

////////////////////////////////////////////////////////////////////////////////

//! References the per-call-site static anchor and its one-shot registration flag.
//! Produced by the lambda embedded in the fluent |YT_TLOG_*| macros.
struct TStaticAnchorRef
{
    TLoggingAnchor* Anchor;
    std::atomic<bool>* Registered;
};

//! Wraps the format spec passed to |TTaggedLoggingGuard::With| and validates at compile
//! time that it is a |%|-prefixed string literal (e.g. |"%v"|, |"%08x"|). The stored
//! spec has the leading |%| stripped, as expected by |FormatValue|.
class TLoggingTagSpec
{
public:
    template <size_t N>
    consteval TLoggingTagSpec(const char (&spec)[N])
        : Spec_(spec + 1, N - 2)
    {
        static_assert(N >= 2, "Logging tag format spec must be a non-empty string literal");
        if (spec[0] != '%') {
            TheLoggingTagFormatSpecMustStartWithPercentSign();
        }
    }

    TStringBuf Get() const
    {
        return Spec_;
    }

private:
    const TStringBuf Spec_;

    // Undefined on purpose: calling it from the |consteval| ctor turns a missing
    // leading |%| into a compile error that names the violated rule.
    static void TheLoggingTagFormatSpecMustStartWithPercentSign();
};

class TWellKnownTaggedLoggingGuard;

//! Accumulates a tagged log message via a fluent |.With| chain and emits the event in
//! its destructor. Instantiated by the fluent |YT_TLOG_*| macros, which guarantee that
//! the chain is reached only when the level is enabled (so tag value expressions are
//! not evaluated otherwise).
//!
//! The user message -- with the logger's contextual (logger/trace/thread) tags folded
//! in -- goes to the payload message field; each |.With(key, value)| becomes a
//! structured payload tag (see #TTaggedPayloadWriter).
class TTaggedLoggingGuard
{
public:
    TTaggedLoggingGuard(
        const TLogger& logger,
        ELogLevel level,
        ::TSourceLocation sourceLocation,
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(
            logger,
            level,
            sourceLocation,
            anchorRef,
            message,
            /*alwaysBuildMessage*/ false)
    { }

    TTaggedLoggingGuard(const TTaggedLoggingGuard&) = delete;
    TTaggedLoggingGuard& operator=(const TTaggedLoggingGuard&) = delete;

    bool IsEnabled() const
    {
        return Enabled_;
    }

    template <class TValue>
    TTaggedLoggingGuard& With(TStringBuf tag, const TValue& value) &
    {
        return DoWith(tag, value, "v"_sb);
    }

    template <class TValue>
    TTaggedLoggingGuard& With(TStringBuf tag, const TValue& value, TLoggingTagSpec spec) &
    {
        return DoWith(tag, value, spec.Get());
    }

    //! Attaches a well-known tag whose key is resolved from #value's type via the
    //! |GetWellKnownLoggingTag| ADL point (the type must opt in, e.g. errors).
    //!
    //! Returns a #TWellKnownTaggedLoggingGuard, which exposes only further well-known
    //! tags: the payload contract requires well-known tags to come last (so
    //! #FormatTaggedPayload can stay single-pass), so a keyed |.With(key, value)| after a
    //! well-known tag must not compile.
    template <class TValue>
    TWellKnownTaggedLoggingGuard With(const TValue& value) &;

    ~TTaggedLoggingGuard()
    {
        if (!Enabled_) {
            return;
        }
        LogEventImpl(
            LoggingContext_,
            Logger_,
            EffectiveLevel_,
            SourceLocation_,
            Anchor_,
            Writer_.Finish());
    }

protected:
    const TLogger& Logger_;
    const ::TSourceLocation SourceLocation_;
    TLoggingAnchor* const Anchor_;

    bool Enabled_ = false;
    ELogLevel EffectiveLevel_ = ELogLevel::Minimum;
    TLoggingContext LoggingContext_;
    TTaggedPayloadWriter Writer_;

    //! Shared constructor. When #alwaysBuildMessage is set the payload message is built
    //! even if the level is disabled (so a terminal guard can still recover it); #Enabled_
    //! continues to gate whether the destructor emits the event.
    TTaggedLoggingGuard(
        const TLogger& logger,
        ELogLevel level,
        ::TSourceLocation sourceLocation,
        TStaticAnchorRef anchorRef,
        TStringBuf message,
        bool alwaysBuildMessage)
        : Logger_(logger)
        , SourceLocation_(sourceLocation)
        , Anchor_(anchorRef.Anchor)
    {
        if (!Logger_.IsAnchorUpToDate(*Anchor_)) [[unlikely]] {
            Logger_.UpdateStaticAnchor(Anchor_, anchorRef.Registered, sourceLocation, message);
        }

        EffectiveLevel_ = TLogger::GetEffectiveLoggingLevel(level, *Anchor_);
        Enabled_ = Logger_.IsLevelEnabled(EffectiveLevel_);
        if (!Enabled_ && !alwaysBuildMessage) {
            return;
        }

        LoggingContext_ = GetLoggingContext();

        auto* builder = Writer_.BeginMessage();
        builder->AppendString(message);
        if (HasMessageTags(LoggingContext_, Logger_)) {
            builder->AppendString(" ("_sb);
            AppendMessageTags(builder, LoggingContext_, Logger_);
            builder->AppendChar(')');
        }
        Writer_.EndMessage();
    }

private:
    template <class TValue>
    TTaggedLoggingGuard& DoWith(TStringBuf tag, const TValue& value, TStringBuf spec) &
    {
        // Format the value straight into the payload buffer; no temporary.
        FormatValue(Writer_.BeginTag(tag), value, spec);
        Writer_.EndTag();
        return *this;
    }
};

//! Restricts the fluent |.With| chain once a well-known tag has been attached. The
//! payload contract requires well-known tags to come last (#FormatTaggedPayload is
//! single-pass), so only further well-known |.With(value)| calls are exposed -- a keyed
//! |.With(key, value)| after a well-known tag fails to compile.
class TWellKnownTaggedLoggingGuard
{
public:
    explicit TWellKnownTaggedLoggingGuard(TTaggedLoggingGuard& guard)
        : Guard_(guard)
    { }

    template <class TValue>
    TWellKnownTaggedLoggingGuard With(const TValue& value) &&
    {
        return Guard_.With(value);
    }

private:
    TTaggedLoggingGuard& Guard_;
};

template <class TValue>
TWellKnownTaggedLoggingGuard TTaggedLoggingGuard::With(const TValue& value) &
{
    FormatValue(Writer_.BeginWellKnownTag(GetWellKnownLoggingTag(value)), value, "v"_sb);
    Writer_.EndTag();
    return TWellKnownTaggedLoggingGuard(*this);
}

//! Terminal guard for the fluent |YT_TLOG_FATAL| macros. Builds the message
//! unconditionally and, once the |.With| chain completes, emits the event at |Fatal|
//! level -- which aborts the process. The enclosing |for| invokes #Commit in its step.
class TTaggedFatalLoggingGuard
    : public TTaggedLoggingGuard
{
public:
    TTaggedFatalLoggingGuard(
        const TLogger& logger,
        ::TSourceLocation sourceLocation,
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(logger, ELogLevel::Fatal, sourceLocation, anchorRef, message, /*alwaysBuildMessage*/ true)
    { }

    //! Returns true exactly once, so the enclosing |for| runs the |.With| chain a single
    //! time before its step expression commits the event.
    bool TryEnter()
    {
        bool pending = Pending_;
        Pending_ = false;
        return pending;
    }

    //! Emits the event at |Fatal| level; the log manager aborts the process.
    [[noreturn]] void Commit() &
    {
        Enabled_ = false; // The event is emitted here, not from the base destructor.
        LogEventImpl(LoggingContext_, Logger_, ELogLevel::Fatal, SourceLocation_, Anchor_, Writer_.Finish());
        Y_UNREACHABLE();
    }

private:
    bool Pending_ = true;
};

//! Terminal guard for the fluent |YT_TLOG_ALERT_AND_THROW| macros. Builds the message
//! unconditionally; once the |.With| chain completes, #Commit emits the event at |Alert|
//! level (when enabled) and returns the message so the macro can attach it to the thrown
//! error. The throw lives in the macro -- the logging library must not depend on the
//! error library, and a destructor must not throw.
class TTaggedThrowingLoggingGuard
    : public TTaggedLoggingGuard
{
public:
    TTaggedThrowingLoggingGuard(
        const TLogger& logger,
        ::TSourceLocation sourceLocation,
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(logger, ELogLevel::Alert, sourceLocation, anchorRef, message, /*alwaysBuildMessage*/ true)
    { }

    //! Returns true exactly once, so the enclosing |for| runs the |.With| chain a single
    //! time before its step expression commits the event and throws.
    bool TryEnter()
    {
        bool pending = Pending_;
        Pending_ = false;
        return pending;
    }

    //! Emits the alert event (when enabled) and returns its message for the error payload.
    std::string Commit() &
    {
        auto payload = Writer_.Finish();
        std::string message(GetMessageFromTaggedPayload(payload));
        if (Enabled_) {
            Enabled_ = false; // The event is emitted here, not from the base destructor.
            LogEventImpl(LoggingContext_, Logger_, EffectiveLevel_, SourceLocation_, Anchor_, std::move(payload));
        }
        return message;
    }

private:
    bool Pending_ = true;
};

//! A no-op stand-in for #TTaggedLoggingGuard used by compile-time-disabled trace logging:
//! it swallows the |.With| chain without evaluating it.
class TNullTaggedLoggingGuard
{
public:
    template <class... TArgs>
    TNullTaggedLoggingGuard& With(TArgs&&...)
    {
        return *this;
    }
};

template <class TMessage>
TNullTaggedLoggingGuard MakeNullTaggedLoggingGuard(const TMessage&)
{
    return {};
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
