#pragma once

#include "defs.h"

#include "log_iface.h"
#include "log_settings.h"
#include "log_metrics.h"
#include "log_buffer.h"
#include "actorsystem.h"
#include "events.h"
#include "event_local.h"
#include "hfunc.h"
#include "mon.h"

#include <util/generic/vector.h>
#include <util/string/printf.h>
#include <util/string/builder.h>
#include <library/cpp/logger/all.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/actors/memory_log/memlog.h>

// TODO: limit number of messages per second
// TODO: make TLogComponentLevelRequest/Response network messages

#define IS_LOG_PRIORITY_ENABLED(priority, component)                           \
    (NActors::TlsActivationContext ? (static_cast<::NActors::NLog::TSettings*>((*NActors::TlsActivationContext).LoggerSettings()) &&            \
     static_cast<::NActors::NLog::TSettings*>((*NActors::TlsActivationContext).LoggerSettings())->Satisfies(   \
             static_cast<::NActors::NLog::EPriority>(priority),                                  \
             static_cast<::NActors::NLog::EComponent>(component),                                \
             0ull)                                                                               \
    ) : true)

#define IS_EMERG_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_EMERG, component)
#define IS_ALERT_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_ALERT, component)
#define IS_CRIT_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_CRIT, component)
#define IS_ERROR_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_ERROR, component)
#define IS_WARN_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_WARN, component)
#define IS_NOTICE_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_NOTICE, component)
#define IS_INFO_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_INFO, component)
#define IS_DEBUG_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_DEBUG, component)
#define IS_TRACE_LOG_ENABLED(component) IS_LOG_PRIORITY_ENABLED(NActors::NLog::PRI_TRACE, component)

#define LOG_LOG_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, ...)                                               \
    do {                                                                                                                       \
        ::NActors::NLog::TSettings* mSettings = static_cast<::NActors::NLog::TSettings*>((actorCtxOrSystem).LoggerSettings()); \
        ::NActors::NLog::EPriority mPriority = static_cast<::NActors::NLog::EPriority>(priority);                              \
        ::NActors::NLog::EComponent mComponent = static_cast<::NActors::NLog::EComponent>(component);                          \
        if (mSettings && mSettings->Satisfies(mPriority, mComponent, sampleBy)) {                                              \
            ::NActors::MemLogAdapter(                                                                                          \
                actorCtxOrSystem, priority, component, __VA_ARGS__);                                                           \
        }                                                                                                                      \
    } while (0) /**/

#define LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, stream)      \
    LOG_LOG_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, [&]() -> TString { \
        TStringBuilder logStringBuilder;                                                   \
        logStringBuilder << stream;                                                        \
        return std::move(logStringBuilder);                                                \
    }())

#define LOG_LOG(actorCtxOrSystem, priority, component, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, priority, component, 0ull, __VA_ARGS__)
#define LOG_LOG_S(actorCtxOrSystem, priority, component, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, 0ull, stream)

// use these macros for logging via actor system or actor context
#define LOG_EMERG(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_EMERG, component, __VA_ARGS__)
#define LOG_ALERT(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_ALERT, component, __VA_ARGS__)
#define LOG_CRIT(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_CRIT, component, __VA_ARGS__)
#define LOG_ERROR(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_ERROR, component, __VA_ARGS__)
#define LOG_WARN(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_WARN, component, __VA_ARGS__)
#define LOG_NOTICE(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, component, __VA_ARGS__)
#define LOG_INFO(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_INFO, component, __VA_ARGS__)
#define LOG_DEBUG(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, __VA_ARGS__)
#define LOG_TRACE(actorCtxOrSystem, component, ...) LOG_LOG(actorCtxOrSystem, NActors::NLog::PRI_TRACE, component, __VA_ARGS__)

#define LOG_EMERG_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_EMERG, component, stream)
#define LOG_ALERT_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_ALERT, component, stream)
#define LOG_CRIT_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_CRIT, component, stream)
#define LOG_ERROR_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_ERROR, component, stream)
#define LOG_WARN_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_WARN, component, stream)
#define LOG_NOTICE_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, component, stream)
#define LOG_INFO_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_INFO, component, stream)
#define LOG_DEBUG_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, stream)
#define LOG_TRACE_S(actorCtxOrSystem, component, stream) LOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_TRACE, component, stream)

#define ALOG_EMERG(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_EMERG, component, stream)
#define ALOG_ALERT(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_ALERT, component, stream)
#define ALOG_CRIT(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_CRIT, component, stream)
#define ALOG_ERROR(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_ERROR, component, stream)
#define ALOG_WARN(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_WARN, component, stream)
#define ALOG_NOTICE(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_NOTICE, component, stream)
#define ALOG_INFO(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_INFO, component, stream)
#define ALOG_DEBUG(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_DEBUG, component, stream)
#define ALOG_TRACE(component, stream) LOG_LOG_S(*NActors::TlsActivationContext, NActors::NLog::PRI_TRACE, component, stream)

#define LOG_EMERG_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_EMERG, component, sampleBy, __VA_ARGS__)
#define LOG_ALERT_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_ALERT, component, sampleBy, __VA_ARGS__)
#define LOG_CRIT_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_CRIT, component, sampleBy, __VA_ARGS__)
#define LOG_ERROR_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_ERROR, component, sampleBy, __VA_ARGS__)
#define LOG_WARN_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_WARN, component, sampleBy, __VA_ARGS__)
#define LOG_NOTICE_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, component, sampleBy, __VA_ARGS__)
#define LOG_INFO_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_INFO, component, sampleBy, __VA_ARGS__)
#define LOG_DEBUG_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, sampleBy, __VA_ARGS__)
#define LOG_TRACE_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, ...) LOG_LOG_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_TRACE, component, sampleBy, __VA_ARGS__)

#define LOG_EMERG_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_EMERG, component, sampleBy, stream)
#define LOG_ALERT_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_ALERT, component, sampleBy, stream)
#define LOG_CRIT_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_CRIT, component, sampleBy, stream)
#define LOG_ERROR_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_ERROR, component, sampleBy, stream)
#define LOG_WARN_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_WARN, component, sampleBy, stream)
#define LOG_NOTICE_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_NOTICE, component, sampleBy, stream)
#define LOG_INFO_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_INFO, component, sampleBy, stream)
#define LOG_DEBUG_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, sampleBy, stream)
#define LOG_TRACE_S_SAMPLED_BY(actorCtxOrSystem, component, sampleBy, stream) LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, NActors::NLog::PRI_TRACE, component, sampleBy, stream)

// Log Throttling
#define LOG_LOG_THROTTLE(throttler, actorCtxOrSystem, priority, component, ...) \
    do {                                                                        \
        if ((throttler).Kick()) {                                               \
            LOG_LOG(actorCtxOrSystem, priority, component, __VA_ARGS__);        \
        }                                                                       \
    } while (0) /**/

#define TRACE_EVENT(component)                                                                                                         \
    const auto& currentTracer = component;                                                                                             \
    if (ev->HasEvent()) {                                                                                                              \
        LOG_TRACE(*TlsActivationContext, currentTracer, "%s, received event# %" PRIu32 ", Sender %s, Recipient %s: %s",                                  \
                  __FUNCTION__, ev->Type, ev->Sender.ToString().data(), SelfId().ToString().data(), ev->ToString().substr(0, 1000).data()); \
    } else {                                                                                                                           \
        LOG_TRACE(*TlsActivationContext, currentTracer, "%s, received event# %" PRIu32 ", Sender %s, Recipient %s",                                      \
                  __FUNCTION__, ev->Type, ev->Sender.ToString().data(), ev->Recipient.ToString().data());                                          \
    }
#define TRACE_EVENT_TYPE(eventType) LOG_TRACE(*TlsActivationContext, currentTracer, "%s, processing event %s", __FUNCTION__, eventType)

class TLog;
class TLogBackend;

namespace NActors {
    class TLoggerActor;

    ////////////////////////////////////////////////////////////////////////////////
    // SET LOG LEVEL FOR A COMPONENT
    ////////////////////////////////////////////////////////////////////////////////
    class TLogComponentLevelRequest: public TEventLocal<TLogComponentLevelRequest, int(NLog::EEv::LevelReq)> {
    public:
        // set given priority for the component
        TLogComponentLevelRequest(NLog::EPriority priority, NLog::EComponent component)
            : Priority(priority)
            , Component(component)
        {
        }

        // set given priority for all components
        TLogComponentLevelRequest(NLog::EPriority priority)
            : Priority(priority)
            , Component(NLog::InvalidComponent)
        {
        }

    protected:
        NLog::EPriority Priority;
        NLog::EComponent Component;

        friend class TLoggerActor;
    };

    class TLogComponentLevelResponse: public TEventLocal<TLogComponentLevelResponse, int(NLog::EEv::LevelResp)> {
    public:
        TLogComponentLevelResponse(int code, const TString& explanation)
            : Code(code)
            , Explanation(explanation)
        {
        }

        int GetCode() const {
            return Code;
        }

        const TString& GetExplanation() const {
            return Explanation;
        }

    protected:
        int Code;
        TString Explanation;
    };

    class TFlushLogBuffer: public TEventLocal<TFlushLogBuffer, int(NLog::EEv::Buffer)> {
    public:
        TFlushLogBuffer() {
        }
    };

    ////////////////////////////////////////////////////////////////////////////////
    // LOGGER ACTOR
    ////////////////////////////////////////////////////////////////////////////////
    class TLoggerActor: public TActor<TLoggerActor> {
    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::LOG_ACTOR;
        }

        TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                     TAutoPtr<TLogBackend> logBackend,
                     TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                     std::shared_ptr<TLogBackend> logBackend,
                     TIntrusivePtr<NMonitoring::TDynamicCounters> counters);
        TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                     TAutoPtr<TLogBackend> logBackend,
                     std::shared_ptr<NMonitoring::TMetricRegistry> metrics);
        TLoggerActor(TIntrusivePtr<NLog::TSettings> settings,
                     std::shared_ptr<TLogBackend> logBackend,
                     std::shared_ptr<NMonitoring::TMetricRegistry> metrics);
        ~TLoggerActor();

        void StateFunc(TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TFlushLogBuffer, FlushLogBufferMessageEvent);
                HFunc(NLog::TEvLog, HandleLogEvent);
                HFunc(TLogComponentLevelRequest, HandleLogComponentLevelRequest);
                HFunc(NMon::TEvHttpInfo, HandleMonInfo);
            }
        }

        STFUNC(StateDefunct) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NLog::TEvLog, HandleLogEventDrop);
                HFunc(TLogComponentLevelRequest, HandleLogComponentLevelRequest);
                HFunc(NMon::TEvHttpInfo, HandleMonInfo);
                cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            }
        }

        // Directly call logger instead of sending a message
        void Log(TInstant time, NLog::EPriority priority, NLog::EComponent component, const char* c, ...);

        static void Throttle(const NLog::TSettings& settings);

    private:
        TIntrusivePtr<NLog::TSettings> Settings;
        std::shared_ptr<TLogBackend> LogBackend;
        ui64 PassedCount = 0;
        TDuration WakeupInterval{TDuration::Seconds(5)};
        std::unique_ptr<ILoggerMetrics> Metrics;
        TLogBuffer LogBuffer;

        void BecomeDefunct();
        void FlushLogBufferMessageEvent(TFlushLogBuffer::TPtr& ev, const NActors::TActorContext& ctx);
        void HandleLogEvent(NLog::TEvLog::TPtr& ev, const TActorContext& ctx);
        void HandleLogEventDrop(const NLog::TEvLog::TPtr& ev);
        void HandleLogComponentLevelRequest(TLogComponentLevelRequest::TPtr& ev, const TActorContext& ctx);
        void HandleMonInfo(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx);
        void HandleWakeup();
        [[nodiscard]] bool OutputRecord(NLog::TEvLog *evLog) noexcept;
        [[nodiscard]] bool OutputRecord(TInstant time, NLog::EPrio priority, NLog::EComponent component, const TString& formatted) noexcept;
        void RenderComponentPriorities(IOutputStream& str);
        void FlushLogBufferMessage();
        void WriteMessageStat(const NLog::TEvLog& ev);
        static const char* FormatLocalTimestamp(TInstant time, char* buf);
    };

    ////////////////////////////////////////////////////////////////////////////////
    // LOG THROTTLING
    // TTrivialLogThrottler -- log a message every 'period' duration
    // Use case:
    //  TTrivialLogThrottler throttler(TDuration::Minutes(1));
    //  ....
    //  LOG_LOG_THROTTLE(throttler, ctx, NActors::NLog::PRI_ERROR, SOME, "Error");
    ////////////////////////////////////////////////////////////////////////////////
    class TTrivialLogThrottler {
    public:
        TTrivialLogThrottler(TDuration period)
            : Period(period)
        {
        }

        // return value:
        // true -- write to log
        // false -- don't write to log, throttle
        bool Kick() {
            auto now = TInstant::Now();
            if (now >= (LastWrite + Period)) {
                LastWrite = now;
                return true;
            } else {
                return false;
            }
        }

    private:
        TInstant LastWrite;
        TDuration Period;
    };

    ////////////////////////////////////////////////////////////////////////////////
    // SYSLOG BACKEND
    ////////////////////////////////////////////////////////////////////////////////
    TAutoPtr<TLogBackend> CreateSysLogBackend(const TString& ident,
                                              bool logPError, bool logCons);
    TAutoPtr<TLogBackend> CreateStderrBackend();
    TAutoPtr<TLogBackend> CreateFileBackend(const TString& fileName);
    TAutoPtr<TLogBackend> CreateNullBackend();
    TAutoPtr<TLogBackend> CreateCompositeLogBackend(TVector<TAutoPtr<TLogBackend>>&& underlyingBackends);

    /////////////////////////////////////////////////////////////////////
    //  Logging adaptors for memory log and logging into filesystem
    /////////////////////////////////////////////////////////////////////

    namespace NDetail {
        inline void Y_PRINTF_FORMAT(2, 3) PrintfV(TString& dst, const char* format, ...) {
            va_list params;
            va_start(params, format);
            vsprintf(dst, format, params);
            va_end(params);
        }

        inline void PrintfV(TString& dst, const char* format, va_list params) {
            vsprintf(dst, format, params);
        }
    } // namespace NDetail

    template <typename TCtx>
    inline void DeliverLogMessage(TCtx& ctx, NLog::EPriority mPriority, NLog::EComponent mComponent, TString &&str)
    {
        const NLog::TSettings *mSettings = ctx.LoggerSettings();
        TLoggerActor::Throttle(*mSettings);
        ctx.Send(new IEventHandle(mSettings->LoggerActorId, TActorId(), new NLog::TEvLog(mPriority, mComponent, std::move(str))));
    }

    template <typename TCtx, typename... TArgs>
    inline void MemLogAdapter(
        TCtx& actorCtxOrSystem,
        NLog::EPriority mPriority,
        NLog::EComponent mComponent,
        const char* format, TArgs&&... params) {
        TString Formatted;


        if constexpr (sizeof... (params) > 0) {
            NDetail::PrintfV(Formatted, format, std::forward<TArgs>(params)...);
        } else {
            NDetail::PrintfV(Formatted, "%s", format);
        }

        MemLogWrite(Formatted.data(), Formatted.size(), true);
        DeliverLogMessage(actorCtxOrSystem, mPriority, mComponent, std::move(Formatted));
    }

    template <typename TCtx>
    Y_WRAPPER inline void MemLogAdapter(
        TCtx& actorCtxOrSystem,
        NLog::EPriority mPriority,
        NLog::EComponent mComponent,
        const TString& str) {

        MemLogWrite(str.data(), str.size(), true);
        DeliverLogMessage(actorCtxOrSystem, mPriority, mComponent, TString(str));
    }

    template <typename TCtx>
    Y_WRAPPER inline void MemLogAdapter(
        TCtx& actorCtxOrSystem,
        NLog::EPriority mPriority,
        NLog::EComponent mComponent,
        TString&& str) {

        MemLogWrite(str.data(), str.size(), true);
        DeliverLogMessage(actorCtxOrSystem, mPriority, mComponent, std::move(str));
    }

    class TRecordWriter: public TStringBuilder {
    private:
        const TActorContext* ActorContext = nullptr;
        ::NActors::NLog::EPriority Priority = ::NActors::NLog::EPriority::PRI_INFO;
        ::NActors::NLog::EComponent Component = 0;
    public:
        TRecordWriter(::NActors::NLog::EPriority priority, ::NActors::NLog::EComponent component)
            : ActorContext(NActors::TlsActivationContext ? &NActors::TlsActivationContext->AsActorContext() : nullptr)
            , Priority(priority)
            , Component(component) {

        }

        ~TRecordWriter() {
            if (ActorContext) {
                ::NActors::MemLogAdapter(*ActorContext, Priority, Component, *this);
            } else {
                Cerr << "FALLBACK_ACTOR_LOGGING;priority=" << Priority << ";component=" << Component << ";" << static_cast<const TStringBuilder&>(*this) << Endl;
            }
        }
    };

    class TFormattedRecordWriter {
    private:
        const TActorContext* ActorContext = nullptr;
        ::NActors::NLog::EPriority Priority = ::NActors::NLog::EPriority::PRI_INFO;
        ::NActors::NLog::EComponent Component = 0;
        TStringBuilder Builder;
    public:

        TFormattedRecordWriter(::NActors::NLog::EPriority priority, ::NActors::NLog::EComponent component)
            : ActorContext(NActors::TlsActivationContext ? &NActors::TlsActivationContext->AsActorContext() : nullptr)
            , Priority(priority)
            , Component(component) {

        }

        template <class TKey, class TValue>
        TFormattedRecordWriter& operator()(const TKey& pName, const TValue& pValue) {
            Builder << pName << "=" << pValue << ";";
            return *this;
        }

        ~TFormattedRecordWriter() {
            if (ActorContext) {
                ::NActors::MemLogAdapter(*ActorContext, Priority, Component, Builder);
            } else {
                Cerr << "FALLBACK_ACTOR_LOGGING;priority=" << Priority << ";component=" << Component << ";" << Builder << Endl;
            }
        }
    };

}

#define ACTORS_FORMATTED_LOG(mPriority, mComponent) \
    if (NActors::TlsActivationContext && !IS_LOG_PRIORITY_ENABLED(mPriority, mComponent));\
        else NActors::TFormattedRecordWriter(\
            static_cast<::NActors::NLog::EPriority>(mPriority), static_cast<::NActors::NLog::EComponent>(mComponent)\
            )("fline", TStringBuilder() << TStringBuf(__FILE__).RAfter(LOCSLASH_C) << ":" << __LINE__)

#define ACTORS_LOG_STREAM(mPriority, mComponent) \
    if (NActors::TlsActivationContext && !IS_LOG_PRIORITY_ENABLED(mPriority, mComponent));\
        else NActors::TRecordWriter(\
            static_cast<::NActors::NLog::EPriority>(mPriority), static_cast<::NActors::NLog::EComponent>(mComponent)\
            ) << TStringBuf(__FILE__).RAfter(LOCSLASH_C) << ":" << __LINE__ << " :"

#define ACTORS_LOG_STREAM_TRACE(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_TRACE, component)
#define ACTORS_LOG_STREAM_DEBUG(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_DEBUG, component)
#define ACTORS_LOG_STREAM_INFO(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_INFO, component)
#define ACTORS_LOG_STREAM_NOTICE(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_NOTICE, component)
#define ACTORS_LOG_STREAM_WARN(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_WARN, component)
#define ACTORS_LOG_STREAM_ERROR(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_ERROR, component)
#define ACTORS_LOG_STREAM_CRIT(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_CRIT, component)
#define ACTORS_LOG_STREAM_ALERT(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_ALERT, component)
#define ACTORS_LOG_STREAM_EMERG(component) ACTORS_LOG_STREAM(NActors::NLog::PRI_EMERG, component)

#define ALS_TRACE(component) ACTORS_LOG_STREAM_TRACE(component)
#define ALS_DEBUG(component) ACTORS_LOG_STREAM_DEBUG(component)
#define ALS_INFO(component) ACTORS_LOG_STREAM_INFO(component)
#define ALS_NOTICE(component) ACTORS_LOG_STREAM_NOTICE(component)
#define ALS_WARN(component) ACTORS_LOG_STREAM_WARN(component)
#define ALS_ERROR(component) ACTORS_LOG_STREAM_ERROR(component)
#define ALS_CRIT(component) ACTORS_LOG_STREAM_CRIT(component)
#define ALS_ALERT(component) ACTORS_LOG_STREAM_ALERT(component)
#define ALS_EMERG(component) ACTORS_LOG_STREAM_EMERG(component)

#define ACTORS_FORMATTED_LOG_TRACE(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_TRACE, component)
#define ACTORS_FORMATTED_LOG_DEBUG(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_DEBUG, component)
#define ACTORS_FORMATTED_LOG_INFO(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_INFO, component)
#define ACTORS_FORMATTED_LOG_NOTICE(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_NOTICE, component)
#define ACTORS_FORMATTED_LOG_WARN(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_WARN, component)
#define ACTORS_FORMATTED_LOG_ERROR(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_ERROR, component)
#define ACTORS_FORMATTED_LOG_CRIT(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_CRIT, component)
#define ACTORS_FORMATTED_LOG_ALERT(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_ALERT, component)
#define ACTORS_FORMATTED_LOG_EMERG(component) ACTORS_FORMATTED_LOG(NActors::NLog::PRI_EMERG, component)

#define AFL_TRACE(component) ACTORS_FORMATTED_LOG_TRACE(component)
#define AFL_DEBUG(component) ACTORS_FORMATTED_LOG_DEBUG(component)
#define AFL_INFO(component) ACTORS_FORMATTED_LOG_INFO(component)
#define AFL_NOTICE(component) ACTORS_FORMATTED_LOG_NOTICE(component)
#define AFL_WARN(component) ACTORS_FORMATTED_LOG_WARN(component)
#define AFL_ERROR(component) ACTORS_FORMATTED_LOG_ERROR(component)
#define AFL_CRIT(component) ACTORS_FORMATTED_LOG_CRIT(component)
#define AFL_ALERT(component) ACTORS_FORMATTED_LOG_ALERT(component)
#define AFL_EMERG(component) ACTORS_FORMATTED_LOG_EMERG(component)
