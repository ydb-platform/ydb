#pragma once

#include "public.h"

#include <library/cpp/yt/string/format.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/cpu_clock/public.h>

#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/misc/thread_name.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <util/system/src_location.h>

#include <util/generic/size_literals.h>

#include <atomic>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr double DefaultStructuredValidationSamplingRate = 0.01;

struct TLoggingCategory
{
    std::string Name;
    //! This value is used for early dropping of plaintext events in order
    //! to reduce load on logging thread for events which are definitely going
    //! to be dropped due to rule setup.
    //! NB: this optimization is used only for plaintext events since structured
    //! logging rate is negligible comparing to the plaintext logging rate.
    std::atomic<ELogLevel> MinPlainTextLevel;
    std::atomic<int> CurrentVersion;
    std::atomic<int>* ActualVersion;
    std::atomic<double> StructuredValidationSamplingRate = DefaultStructuredValidationSamplingRate;
};

////////////////////////////////////////////////////////////////////////////////

struct TLoggingAnchor
{
    std::atomic<bool> Registered = false;
    TLoggingAnchor* NextAnchor = nullptr;

    ::TSourceLocation SourceLocation = {TStringBuf{}, 0};
    std::string AnchorMessage;

    std::atomic<int> CurrentVersion = 0;

    std::atomic<bool> Suppressed = false;

    std::atomic<std::optional<ELogLevel>> LevelOverride;
    static_assert(decltype(LevelOverride)::is_always_lock_free);

    struct TCounter
    {
        i64 Current = 0;
        i64 Previous = 0;
    };

    TCounter MessageCounter;
    TCounter ByteCounter;
};

////////////////////////////////////////////////////////////////////////////////
// Declare some type aliases to avoid circular dependencies.
using TThreadId = size_t;
using TFiberId = size_t;
using TTraceId = TGuid;
using TRequestId = TGuid;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogMessageKind,
    (Unstructured)
    (Structured)
);

struct TLogEvent
{
    const TLoggingCategory* Category = nullptr;
    ELogLevel Level = ELogLevel::Minimum;
    ELogFamily Family = ELogFamily::PlainText;
    bool Essential = false;

    ELogMessageKind MessageKind = ELogMessageKind::Unstructured;
    TSharedRef MessageRef;

    TCpuInstant Instant = 0;

    TThreadId ThreadId = {};
    TThreadName ThreadName = {};

    TFiberId FiberId = {};

    TTraceId TraceId;
    TRequestId RequestId;

    TStringBuf SourceFile;
    int SourceLine = -1;

    TLoggingAnchor* Anchor = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct ILogManager
{
    virtual ~ILogManager() = default;

    virtual void RegisterStaticAnchor(
        TLoggingAnchor* anchor,
        ::TSourceLocation sourceLocation,
        TStringBuf anchorMessage) = 0;
    virtual void UpdateAnchor(TLoggingAnchor* anchor) = 0;

    virtual void Enqueue(TLogEvent&& event) = 0;

    virtual const TLoggingCategory* GetCategory(TStringBuf categoryName) = 0;
    virtual void UpdateCategory(TLoggingCategory* category) = 0;

    virtual bool GetAbortOnAlert() const = 0;
};

ILogManager* GetDefaultLogManager();

////////////////////////////////////////////////////////////////////////////////

struct TLoggingContext
{
    TCpuInstant Instant;
    TThreadId ThreadId;
    TThreadName ThreadName;
    TFiberId FiberId;
    TTraceId TraceId;
    TRequestId RequestId;
    TStringBuf TraceLoggingTag;
};

TLoggingContext GetLoggingContext();

////////////////////////////////////////////////////////////////////////////////

//! Sets the minimum logging level for messages in current thread.
// NB: In fiber environment, min log level is attached to a fiber,
// so after context switch thread min log level might change.
void SetThreadMinLogLevel(ELogLevel minLogLevel);
ELogLevel GetThreadMinLogLevel();

////////////////////////////////////////////////////////////////////////////////

//! Sets an extra tag for messages in current thread.
// NB: Same as above, in fiber environment messages tags
// are attached to a fiber.
void SetThreadMessageTag(std::string messageTag);
std::string& GetThreadMessageTag();

////////////////////////////////////////////////////////////////////////////////

static constexpr auto NullLoggerMinLevel = ELogLevel::Maximum;

// Min level for non-null logger depends on whether we are in debug or release build.
// - For release mode default behavior is to omit trace logging,
//   this is done by setting logger min level to Debug by default.
// - For debug mode logger min level is set to trace by default, so that trace logging is
//   allowed by logger, but still may be discarded by category min level.
#ifdef NDEBUG
static constexpr auto LoggerDefaultMinLevel = ELogLevel::Debug;
#else
static constexpr auto LoggerDefaultMinLevel = ELogLevel::Trace;
#endif

class TLogger
{
public:
    using TStructuredValidator = std::function<void(const NYson::TYsonString&)>;
    using TStructuredValidators = std::vector<TStructuredValidator>;

    using TStructuredTag = std::pair<std::string, NYson::TYsonString>;
    // TODO(max42): switch to TCompactVector after YT-15430.
    using TStructuredTags = std::vector<TStructuredTag>;

    TLogger() = default;
    TLogger(const TLogger& other) = default;
    TLogger(TLogger&& other) = default;
    TLogger& operator=(const TLogger& other) = default;
    TLogger& operator=(TLogger&& other) = default;

    TLogger(ILogManager* logManager, TStringBuf categoryName);
    explicit TLogger(TStringBuf categoryName);

    explicit operator bool() const;

    //! Enables using |Logger| in YT_LOG_* macros as both data members and functions
    //! (e.g. those introduced by YT_DEFINE_GLOBAL).
    const TLogger& operator()() const;

    const TLoggingCategory* GetCategory() const;

    //! Combines given #level and the override from #anchor.
    static ELogLevel GetEffectiveLoggingLevel(ELogLevel level, const TLoggingAnchor& anchor);

    //! Validate that level is admitted by logger's own min level
    //! and by category's min level.
    bool IsLevelEnabled(ELogLevel level) const;

    bool GetAbortOnAlert() const;

    bool IsEssential() const;

    bool IsAnchorUpToDate(const TLoggingAnchor& anchor) const;
    void UpdateStaticAnchor(
        TLoggingAnchor* anchor,
        std::atomic<bool>* anchorRegistered,
        ::TSourceLocation sourceLocation,
        TStringBuf message) const;
    void UpdateDynamicAnchor(TLoggingAnchor* anchor) const;

    void Write(TLogEvent&& event) const;

    void AddRawTag(const std::string& tag);
    template <class... TArgs>
    void AddTag(const char* format, TArgs&&... args);

    template <class TType>
    void AddStructuredTag(TStringBuf key, TType value);

    void AddStructuredValidator(TStructuredValidator validator);

    TLogger WithRawTag(const std::string& tag) const &;
    TLogger WithRawTag(const std::string& tag) &&;
    template <class... TArgs>
    TLogger WithTag(const char* format, TArgs&&... args) const &;
    template <class... TArgs>
    TLogger WithTag(const char* format, TArgs&&... args) &&;

    template <class TType>
    TLogger WithStructuredTag(TStringBuf key, TType value) const &;
    template <class TType>
    TLogger WithStructuredTag(TStringBuf key, TType value) &&;

    TLogger WithStructuredValidator(TStructuredValidator validator) const &;
    TLogger WithStructuredValidator(TStructuredValidator validator) &&;

    TLogger WithMinLevel(ELogLevel minLevel) const &;
    TLogger WithMinLevel(ELogLevel minLevel) &&;

    TLogger WithEssential(bool essential = true) const &;
    TLogger WithEssential(bool essential = true) &&;

    const std::string& GetTag() const;
    const TStructuredTags& GetStructuredTags() const;

    const TStructuredValidators& GetStructuredValidators() const;

protected:
    // NB: Mind TSerializableLogger when changing the state.
    // These fields are set only during logger creation, so they are effectively const
    // and accessing them is thread-safe.
    ILogManager* LogManager_ = nullptr;
    const TLoggingCategory* Category_ = nullptr;
    bool Essential_ = false;
    ELogLevel MinLevel_ = NullLoggerMinLevel;

    struct TCoWState final
    {
        std::string Tag;
        TStructuredTags StructuredTags;
        TStructuredValidators StructuredValidators;
    };

    TIntrusivePtr<const TCoWState> CoWState_;

    TCoWState* GetMutableCoWState();
    void ResetCoWState();

private:
    void UpdateCategory() const;
};

////////////////////////////////////////////////////////////////////////////////

void LogStructuredEvent(
    const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_TRACE_LOGGING
#define YT_LOG_TRACE(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Trace, __VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        if (condition)    YT_LOG_TRACE(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    if (!(condition)) YT_LOG_TRACE(__VA_ARGS__)
#else
#define YT_LOG_UNUSED(...)                     if (true) { } else { YT_LOG_DEBUG(__VA_ARGS__); }
#define YT_LOG_TRACE(...)                      YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_IF(condition, ...)        YT_LOG_UNUSED(__VA_ARGS__)
#define YT_LOG_TRACE_UNLESS(condition, ...)    YT_LOG_UNUSED(__VA_ARGS__)
#endif

#define YT_LOG_DEBUG(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Debug, __VA_ARGS__)
#define YT_LOG_DEBUG_IF(condition, ...)        if (condition)    YT_LOG_DEBUG(__VA_ARGS__)
#define YT_LOG_DEBUG_UNLESS(condition, ...)    if (!(condition)) YT_LOG_DEBUG(__VA_ARGS__)

#define YT_LOG_INFO(...)                       YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Info, __VA_ARGS__)
#define YT_LOG_INFO_IF(condition, ...)         if (condition)    YT_LOG_INFO(__VA_ARGS__)
#define YT_LOG_INFO_UNLESS(condition, ...)     if (!(condition)) YT_LOG_INFO(__VA_ARGS__)

#define YT_LOG_WARNING(...)                    YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Warning, __VA_ARGS__)
#define YT_LOG_WARNING_IF(condition, ...)      if (condition)    YT_LOG_WARNING(__VA_ARGS__)
#define YT_LOG_WARNING_UNLESS(condition, ...)  if (!(condition)) YT_LOG_WARNING(__VA_ARGS__)

#define YT_LOG_ERROR(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Error, __VA_ARGS__)
#define YT_LOG_ERROR_IF(condition, ...)        if (condition)    YT_LOG_ERROR(__VA_ARGS__)
#define YT_LOG_ERROR_UNLESS(condition, ...)    if (!(condition)) YT_LOG_ERROR(__VA_ARGS__)

#define YT_LOG_ALERT(...)                      YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Alert, __VA_ARGS__)
#define YT_LOG_ALERT_IF(condition, ...)        if (condition)    YT_LOG_ALERT(__VA_ARGS__)
#define YT_LOG_ALERT_UNLESS(condition, ...)    if (!(condition)) YT_LOG_ALERT(__VA_ARGS__)

#define YT_LOG_FATAL(...) \
    do { \
        YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Fatal, __VA_ARGS__); \
        Y_UNREACHABLE(); \
    } while(false)
#define YT_LOG_FATAL_IF(condition, ...)        if (Y_UNLIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)
#define YT_LOG_FATAL_UNLESS(condition, ...)    if (!Y_LIKELY(condition)) YT_LOG_FATAL(__VA_ARGS__)

/*
 * A few noteworthy observations:
 * 1. This function is meant to be used in places where YT_VERIFY won't look out of place, but
 *    it's safe to just throw an exception. Because of this, this error should not be handled by
 *    clients in any specific way.
 * 2. Most places where this could be used have trace_id enabled, which would make it easy for
 *    administrators to find a correlation between an alert and an error, if a user were to report it.
 * 3. Administrators will receive this alert, so there is no need to enrich the error with
 *    additional information.
 */
#define YT_LOG_ALERT_AND_THROW(...) \
    YT_LOG_EVENT(Logger, ::NYT::NLogging::ELogLevel::Alert, __VA_ARGS__); \
    THROW_ERROR_EXCEPTION( \
        ::NYT::EErrorCode::Fatal, \
        "Malformed request or incorrect state detected")

#define YT_LOG_ALERT_AND_THROW_IF(condition, ...) \
    if (Y_UNLIKELY(condition)) { \
        YT_LOG_ALERT_AND_THROW(__VA_ARGS__); \
    } \
    static_assert(true)

#define YT_LOG_ALERT_AND_THROW_UNLESS(condition, ...) \
    if (!Y_UNLIKELY(condition)) { \
        YT_LOG_ALERT_AND_THROW(__VA_ARGS__); \
    } \
    static_assert(true)

#define YT_LOG_EVENT(logger, level, ...) \
    do { \
        const auto& logger__ = (logger)(); \
        auto level__ = (level); \
        auto location__ = __LOCATION__; \
        static ::NYT::TLeakyStorage<::NYT::NLogging::TLoggingAnchor> anchorStorage__; \
        auto* anchor__ = anchorStorage__.Get(); \
        \
        bool anchorUpToDate__ = logger__.IsAnchorUpToDate(*anchor__); \
        [[likely]] if (anchorUpToDate__) { \
            auto effectiveLevel__ = ::NYT::NLogging::TLogger::GetEffectiveLoggingLevel(level__, *anchor__); \
            if (!logger__.IsLevelEnabled(effectiveLevel__)) { \
                break; \
            } \
        } \
        \
        auto loggingContext__ = ::NYT::NLogging::GetLoggingContext(); \
        auto message__ = ::NYT::NLogging::NDetail::BuildLogMessage(loggingContext__, logger__, __VA_ARGS__); \
        \
        [[unlikely]] if (!anchorUpToDate__) { \
            static std::atomic<bool> anchorRegistered__; \
            logger__.UpdateStaticAnchor(anchor__, &anchorRegistered__, location__, message__.Anchor); \
        } \
        \
        auto effectiveLevel__ = ::NYT::NLogging::TLogger::GetEffectiveLoggingLevel(level__, *anchor__); \
        if (!logger__.IsLevelEnabled(effectiveLevel__)) { \
            break; \
        } \
        \
        ::NYT::NLogging::NDetail::LogEventImpl( \
            loggingContext__, \
            logger__, \
            effectiveLevel__, \
            location__, \
            anchor__, \
            std::move(message__.MessageRef)); \
    } while (false)

#define YT_LOG_EVENT_WITH_DYNAMIC_ANCHOR(logger, level, anchor, ...) \
    do { \
        const auto& logger__ = (logger)(); \
        auto level__ = (level); \
        auto location__ = __LOCATION__; \
        auto* anchor__ = (anchor); \
        \
        bool anchorUpToDate__ = logger__.IsAnchorUpToDate(*anchor__); \
        [[unlikely]] if (!anchorUpToDate__) { \
            logger__.UpdateDynamicAnchor(anchor__); \
        } \
        \
        auto effectiveLevel__ = ::NYT::NLogging::TLogger::GetEffectiveLoggingLevel(level__, *anchor__); \
        if (!logger__.IsLevelEnabled(effectiveLevel__)) { \
            break; \
        } \
        \
        auto loggingContext__ = ::NYT::NLogging::GetLoggingContext(); \
        auto message__ = ::NYT::NLogging::NDetail::BuildLogMessage(loggingContext__, logger__, __VA_ARGS__); \
        \
        ::NYT::NLogging::NDetail::LogEventImpl( \
            loggingContext__, \
            logger__, \
            effectiveLevel__, \
            location__, \
            anchor__, \
            std::move(message__.MessageRef)); \
    } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define LOGGER_INL_H_
#include "logger-inl.h"
#undef LOGGER_INL_H_
