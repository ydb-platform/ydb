#ifndef LOGGER_INL_H_
#error "Direct inclusion of this file is not allowed, include logger.h"
// For the sake of sane code completion.
#include "logger.h"
#endif

#include "tagged_payload.h"
#include "tag.h"

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

template <class TValue>
TLogger& TLogger::AddTag(TLoggingTagKey key, const TValue& value)
{
    GetMutableCoWState()->Tags.Add(key, value);
    return *this;
}

template <class... TArgs>
TLogger& TLogger::AddTagFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args)
{
    GetMutableCoWState()->Tags.AddFormat(key, format, std::forward<TArgs>(args)...);
    return *this;
}

template <class TType>
void TLogger::AddStructuredTag(TStringBuf key, TType value)
{
    auto* state = GetMutableCoWState();
    state->StructuredTags.emplace_back(key, NYson::ConvertToYsonString(value));
}

template <class TValue>
TLogger TLogger::WithTag(TLoggingTagKey key, const TValue& value) const &
{
    auto result = *this;
    result.AddTag(key, value);
    return result;
}

template <class TValue>
TLogger TLogger::WithTag(TLoggingTagKey key, const TValue& value) &&
{
    AddTag(key, value);
    return std::move(*this);
}

template <class... TArgs>
TLogger TLogger::WithTagFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args) const &
{
    auto result = *this;
    result.AddTagFormat(key, format, std::forward<TArgs>(args)...);
    return result;
}

template <class... TArgs>
TLogger TLogger::WithTagFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args) &&
{
    AddTagFormat(key, format, std::forward<TArgs>(args)...);
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
    if (!logger.GetTags().IsEmpty()) {
        return true;
    }
    if (!loggingContext.TraceLoggingTags.Underlying().empty()) {
        return true;
    }
    if (!GetThreadMessageTags().IsEmpty()) {
        return true;
    }
    return false;
}

//! Splices the contextual tag sections into #writer, in the order they are rendered by
//! #AppendMessageTags. Must precede any well-known tag.
inline void AppendContextualTags(
    TTaggedPayloadWriter* writer,
    const TLoggingContext& loggingContext,
    const TLogger& logger)
{
    writer->AppendTags(AsView(logger.GetTags().GetPayload()));
    writer->AppendTags(loggingContext.TraceLoggingTags);
    writer->AppendTags(AsView(GetThreadMessageTags().GetPayload()));
}

inline void AppendMessageTags(
    TStringBuilderBase* builder,
    const TLoggingContext& loggingContext,
    const TLogger& logger)
{
    bool printComma = false;
    auto append = [&] (TLoggingTagListPayloadView tags) {
        if (tags.Underlying().empty()) {
            return;
        }
        if (printComma) {
            builder->AppendString(", "_sb);
        }
        FormatValue(builder, tags, "v"_sb);
        printComma = true;
    };
    append(AsView(logger.GetTags().GetPayload()));
    append(loggingContext.TraceLoggingTags);
    append(AsView(GetThreadMessageTags().GetPayload()));
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

//! Identifies a call site.
struct TStaticAnchorRef
{
    TLoggingAnchor* Anchor;
    std::atomic<bool>* Registered;
    ::TSourceLocation SourceLocation;
};

struct TDynamicAnchorRef
{
    TLoggingAnchor* Anchor;
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
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(
            logger,
            level,
            anchorRef,
            message,
            /*alwaysBuildMessage*/ false)
    { }

    TTaggedLoggingGuard(
        const TLogger& logger,
        ELogLevel level,
        ::TSourceLocation sourceLocation,
        TDynamicAnchorRef anchorRef,
        TStringBuf message)
        : Logger_(logger)
        , SourceLocation_(sourceLocation)
        , Anchor_(anchorRef.Anchor)
    {
        if (!Logger_.IsAnchorUpToDate(*Anchor_)) [[unlikely]] {
            Logger_.UpdateDynamicAnchor(Anchor_);
        }

        Initialize(level, message, /*alwaysBuildMessage*/ false);
    }

    TTaggedLoggingGuard(const TTaggedLoggingGuard&) = delete;
    TTaggedLoggingGuard& operator=(const TTaggedLoggingGuard&) = delete;

    bool IsEnabled() const
    {
        return Enabled_;
    }

    //! The fluent macros end their expansion in a call to this instead of naming the guard
    //! directly. A tag-less |YT_TLOG_INFO("Message");| would otherwise expand to a discarded
    //! id-expression, and -Wunused-value fires on those whenever the call is spelled inside
    //! a macro argument (e.g. within |BIND(...)|) rather than a macro body.
    TTaggedLoggingGuard& Self() &
    {
        return *this;
    }

    template <class TValue>
    TTaggedLoggingGuard& With(TLoggingTagKey tag, const TValue& value) &
    {
        return DoWith(tag, value, "v"_sb);
    }

    //! Attaches the tag only when #condition holds, for fields a message omits rather
    //! than renders empty. NB: #value is evaluated either way.
    template <class TValue>
    TTaggedLoggingGuard& WithIf(bool condition, TLoggingTagKey tag, const TValue& value) &
    {
        return condition ? DoWith(tag, value, "v"_sb) : *this;
    }

    //! Attaches a keyed tag composed from several values, e.g. |.WithFormat("Method", "%v.%v", service, method)|.
    template <class... TArgs>
    TTaggedLoggingGuard& WithFormat(TLoggingTagKey tag, TFormatString<TArgs...> format, TArgs&&... args) &
    {
        Format(Writer_.BeginTag(tag.Get()), format, std::forward<TArgs>(args)...);
        Writer_.EndTag();
        return *this;
    }

    //! Attaches a composed tag only when #condition holds. NB: #args are evaluated either way.
    template <class... TArgs>
    TTaggedLoggingGuard& WithFormatIf(bool condition, TLoggingTagKey tag, TFormatString<TArgs...> format, TArgs&&... args) &
    {
        return condition
            ? WithFormat(tag, format, std::forward<TArgs>(args)...)
            : *this;
    }

    //! Splices a pre-built list of keyed tags, preserving them as individual tags. Chosen
    //! over the well-known single-argument |With| below by exact match.
    TTaggedLoggingGuard& With(const TLoggingTagList& tags) &
    {
        Writer_.AppendTags(AsView(tags.GetPayload()));
        return *this;
    }

    //! Attaches a well-known tag whose key comes from #TWellKnownLoggingTagTraits.
    //!
    //! Returns a #TWellKnownTaggedLoggingGuard, which exposes only further well-known
    //! tags: the payload contract requires well-known tags to come last (so
    //! #FormatTaggedPayload can stay single-pass), so a keyed |.With(key, value)| after a
    //! well-known tag must not compile.
    template <class TValue>
    TWellKnownTaggedLoggingGuard With(const TValue& value) &;

    ~TTaggedLoggingGuard()
    {
        if (Enabled_) {
            Emit(EffectiveLevel_, Writer_.Finish());
        }
    }

protected:
    const TLogger& Logger_;
    const ::TSourceLocation SourceLocation_;
    TLoggingAnchor* const Anchor_;

    bool Enabled_ = false;
    ELogLevel EffectiveLevel_ = ELogLevel::Minimum;
    TLoggingContext LoggingContext_;
    TTaggedPayloadWriter Writer_;

    //! Emits #payload, disarming the destructor so the event is logged exactly once.
    void Emit(ELogLevel level, TTaggedLogEventPayload&& payload)
    {
        Enabled_ = false;
        LogEventImpl(LoggingContext_, Logger_, level, SourceLocation_, Anchor_, std::move(payload));
    }

    //! Shared constructor. When #alwaysBuildMessage is set the payload message is built
    //! even if the level is disabled (so a terminal guard can still recover it); #Enabled_
    //! continues to gate whether the destructor emits the event.
    TTaggedLoggingGuard(
        const TLogger& logger,
        ELogLevel level,
        TStaticAnchorRef anchorRef,
        TStringBuf message,
        bool alwaysBuildMessage)
        : Logger_(logger)
        , SourceLocation_(anchorRef.SourceLocation)
        , Anchor_(anchorRef.Anchor)
    {
        if (!Logger_.IsAnchorUpToDate(*Anchor_)) [[unlikely]] {
            Logger_.UpdateStaticAnchor(Anchor_, anchorRef.Registered, SourceLocation_, message);
        }

        Initialize(level, message, alwaysBuildMessage);
    }

private:
    void Initialize(ELogLevel level, TStringBuf message, bool alwaysBuildMessage)
    {
        EffectiveLevel_ = TLogger::GetEffectiveLoggingLevel(level, *Anchor_);
        Enabled_ = Logger_.IsLevelEnabled(EffectiveLevel_);
        if (!Enabled_ && !alwaysBuildMessage) {
            return;
        }

        LoggingContext_ = GetLoggingContext();

        Writer_.BeginMessage()->AppendString(message);
        Writer_.EndMessage();
        // Contextual tags stay structured here, rather than being folded into the message
        // text as the legacy YT_LOG_* path has to do.
        AppendContextualTags(&Writer_, LoggingContext_, Logger_);
    }

    template <class TValue>
    TTaggedLoggingGuard& DoWith(TLoggingTagKey tag, const TValue& value, TStringBuf spec) &
    {
        // Format the value straight into the payload buffer; no temporary.
        FormatValue(Writer_.BeginTag(tag.Get()), value, spec);
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
    FormatValue(Writer_.BeginWellKnownTag(TWellKnownLoggingTagTraits<TValue>::Key), value, "v"_sb);
    Writer_.EndTag();
    return TWellKnownTaggedLoggingGuard(*this);
}

//! Terminal guard for the fluent |YT_TLOG_FATAL| macros. Builds the message
//! unconditionally and, once the |.With| chain completes, emits the event at |Fatal|
//! level -- which aborts the process. The enclosing |for| invokes #Commit in its step;
//! since #Commit is |[[noreturn]]|, the body runs a single time.
class TTaggedFatalLoggingGuard
    : public TTaggedLoggingGuard
{
public:
    TTaggedFatalLoggingGuard(
        const TLogger& logger,
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(logger, ELogLevel::Fatal, anchorRef, message, /*alwaysBuildMessage*/ true)
    { }

    //! Emits the event at |Fatal| level; the log manager aborts the process.
    [[noreturn]] void Commit() &
    {
        Emit(ELogLevel::Fatal, Writer_.Finish());
        Y_UNREACHABLE();
    }
};

//! Terminal guard for the fluent |YT_TLOG_ALERT_AND_THROW| macros. Builds the message
//! unconditionally; once the |.With| chain completes, #Commit emits the event at |Alert|
//! level (when enabled) and returns it rendered -- tags included -- for the macro to
//! attach to the thrown error. The throw lives in the macro -- the logging library must
//! not depend on the error library, and a destructor must not throw.
class TTaggedThrowingLoggingGuard
    : public TTaggedLoggingGuard
{
public:
    TTaggedThrowingLoggingGuard(
        const TLogger& logger,
        TStaticAnchorRef anchorRef,
        TStringBuf message)
        : TTaggedLoggingGuard(logger, ELogLevel::Alert, anchorRef, message, /*alwaysBuildMessage*/ true)
    { }

    //! Returns true exactly once, so the enclosing |for| runs the |.With| chain a single
    //! time before its step expression commits the event and throws.
    bool TryEnter()
    {
        bool pending = Pending_;
        Pending_ = false;
        return pending;
    }

    //! Emits the alert event (when enabled) and returns it rendered, tags included.
    std::string Commit() &
    {
        auto payload = Writer_.Finish();
        auto message = FormatTaggedPayload(payload);
        if (Enabled_) {
            Emit(EffectiveLevel_, std::move(payload));
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

    template <class... TArgs>
    TNullTaggedLoggingGuard& WithIf(TArgs&&...)
    {
        return *this;
    }

    template <class... TArgs>
    TNullTaggedLoggingGuard& WithFormat(TArgs&&...)
    {
        return *this;
    }

    template <class... TArgs>
    TNullTaggedLoggingGuard& WithFormatIf(TArgs&&...)
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
