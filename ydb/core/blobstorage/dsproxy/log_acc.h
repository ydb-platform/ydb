#pragma once

#include "defs.h"
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

    struct TLogAccumulator {
        struct TLogLine {
            NLog::EPriority Priority;
            NLog::EComponent Component;
            TString Formatted;

            TLogLine(NLog::EPriority priority, NLog::EComponent component, TString formatted)
                : Priority(priority)
                , Component(component)
                , Formatted(formatted)
            {}

        };

        bool IsEnabled;
        bool IsLogEnabled;
        TInstant CreatedAt;
        TDeque<TLogLine> Lines;

        TLogAccumulator()
                : IsEnabled(false)
                , IsLogEnabled(true)
                , CreatedAt(TAppData::TimeProvider->Now())
        {}

        TLogAccumulator(bool isEnabled)
                : IsEnabled(isEnabled)
                , IsLogEnabled(true)
                , CreatedAt(TAppData::TimeProvider->Now())
        {}

        void Clear() {
            CreatedAt = TAppData::TimeProvider->Now();
            Lines.clear();
        }

        void Add(NLog::EPriority priority, NLog::EComponent component, TString formatted) {
            Lines.push_back(TLogLine(priority, component, formatted));
        }

        void UnleashUponLogger() {
            NLog::TSettings * const loggerSettings = TlsActivationContext->ExecutorThread.ActorSystem->LoggerSettings();
            if (!loggerSettings) {
                return;
            }
            TActorId logger = loggerSettings->LoggerActorId;
            for (auto it = Lines.begin(); it != Lines.end(); ++it) {
                TActivationContext::Send(new IEventHandle(logger, TActorId(), new ::NActors::NLog::TEvLog(it->Priority, it->Component, it->Formatted)));
            }
            Lines.clear();
        }

        ui64 LifeMs() {
            return (TAppData::TimeProvider->Now() - CreatedAt).MilliSeconds();
        }

        void Output(IOutputStream &out) {
            for (auto &line: Lines) {
                const auto prio = NActors::NLog::EPrio(line.Priority);
                out << ::NActors::NLog::PriorityToString(prio) << ": " << line.Formatted << Endl;
            }
        }
    };

    struct TLogContext {
        const TString RequestPrefix;
        const NKikimrServices::EServiceKikimr LogComponent;
        TLogAccumulator LogAcc;
        bool SuppressLog = false;

        TLogContext(NKikimrServices::EServiceKikimr logComponent, bool logAccEnabled)
            : RequestPrefix(Sprintf("[%016" PRIx64 "]", TAppData::RandomProvider->GenRand64()))
            , LogComponent(logComponent)
            , LogAcc(logAccEnabled)
        {}
    };

#define ENABLE_LOG_ACCUMULATION 0
#define ENABLE_ALTERNATIVE_LOG_ACCUMULATION 1

#if ENABLE_LOG_ACCUMULATION
    #define A_LOG_LOG_S_IMPL(accumulator, priority, component, stream) \
        do { \
            ::NActors::NLog::TSettings *mSettings = \
                (::NActors::NLog::TSettings*)(TActivationContext::LoggerSettings()); \
            if (mSettings) { \
                ::NActors::NLog::EPriority mPriority = (::NActors::NLog::EPriority)(priority); \
                ::NActors::NLog::EComponent mComponent = (::NActors::NLog::EComponent)(component); \
                if (mSettings->Satisfies(mPriority, mComponent, 0ul)) { \
                    if (isRelease) { \
                        (accumulator).UnleashUponLogger(); \
                    } \
                    TStringBuilder logStringBuilder; \
                    logStringBuilder << stream; \
                    TActivationContext::Send(new IEventHandle(mSettings->LoggerActorId, TActorId(), \
                        new ::NActors::NLog::TEvLog(mPriority, mComponent, (TString)logStringBuilder)); \
                } else { \
                    if (!(isRelease)) { \
                        TStringBuilder logStringBuilder; \
                        logStringBuilder << "(R) " << (accumulator).LifeMs() << " "; \
                        logStringBuilder << stream; \
                        (accumulator).Add(mPriority, mComponent, (TString)logStringBuilder); \
                    }\
                }\
            } \
        } while (0)
#elif ENABLE_ALTERNATIVE_LOG_ACCUMULATION
    #define A_LOG_LOG_S_IMPL(accumulator, priority, component, stream) \
        do { \
            if ((accumulator).IsEnabled) { \
                ::NActors::NLog::EPriority mPriorityAcc = (::NActors::NLog::EPriority)(priority); \
                ::NActors::NLog::EComponent mComponentAcc = (::NActors::NLog::EComponent)(component); \
                TStringBuilder logStringBuilderAcc2; \
                logStringBuilderAcc2 << stream; \
                TStringBuilder logStringBuilderAcc; \
                logStringBuilderAcc << "(R) " << (accumulator).LifeMs() << " "; \
                logStringBuilderAcc << (TString)logStringBuilderAcc2; \
                (accumulator).Add(mPriorityAcc, mComponentAcc, (TString)logStringBuilderAcc); \
                if ((accumulator).IsLogEnabled) { \
                    LOG_LOG_S(*TlsActivationContext, mPriorityAcc, mComponentAcc, (TString)logStringBuilderAcc2); \
                } \
            } else { \
                if ((accumulator).IsLogEnabled) { \
                    LOG_LOG_S(*TlsActivationContext, priority, (component), stream); \
                } \
            } \
        } while (0)
#else
    #define A_LOG_LOG_S_IMPL(accumulator, priority, component, stream) \
        do { \
            LOG_LOG_S(*TlsActivationContext, priority, component, stream); \
        } while (0)
#endif

#define DSP_LOG_LOG_SX(logCtx, priority, marker, stream) \
    do { \
        auto& lc = (logCtx); \
        if (lc.SuppressLog) { \
            break; \
        } \
        A_LOG_LOG_S_IMPL(lc.LogAcc, priority, lc.LogComponent, \
            lc.RequestPrefix << " " << stream << " Marker# " << marker); \
    } while (false)

#define DSP_LOG_LOG_S(priority, marker, stream) \
    DSP_LOG_LOG_SX(LogCtx, priority, marker, stream)

#define R_LOG_EMERG_S(marker, stream)  \
    DSP_LOG_LOG_S(NActors::NLog::PRI_EMERG, marker, stream)
#define R_LOG_ALERT_S(marker, stream)  \
    DSP_LOG_LOG_S(NActors::NLog::PRI_ALERT, marker, stream)
#define R_LOG_CRIT_S(marker, stream)   \
    DSP_LOG_LOG_S(NActors::NLog::PRI_CRIT, marker, stream)
#define R_LOG_ERROR_S(marker, stream)  \
    DSP_LOG_LOG_S(NActors::NLog::PRI_ERROR, marker, stream)
#define R_LOG_WARN_S(marker, stream)   \
    DSP_LOG_LOG_S(NActors::NLog::PRI_WARN, marker, stream)
#define R_LOG_NOTICE_S(marker, stream) \
    DSP_LOG_LOG_S(NActors::NLog::PRI_NOTICE, marker, stream)
#define R_LOG_INFO_S(marker, stream)   \
    DSP_LOG_LOG_S(NActors::NLog::PRI_INFO, marker, stream)
#define R_LOG_DEBUG_S(marker, stream)  \
    DSP_LOG_LOG_S(NActors::NLog::PRI_DEBUG, marker, stream)

#define R_LOG_EMERG_SX(logCtx, marker, stream)  \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_EMERG, marker, stream)
#define R_LOG_ALERT_SX(logCtx, marker, stream)  \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_ALERT, marker, stream)
#define R_LOG_CRIT_SX(logCtx, marker, stream)   \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_CRIT, marker, stream)
#define R_LOG_ERROR_SX(logCtx, marker, stream)  \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_ERROR, marker, stream)
#define R_LOG_WARN_SX(logCtx, marker, stream)   \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_WARN, marker, stream)
#define R_LOG_NOTICE_SX(logCtx, marker, stream) \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_NOTICE, marker, stream)
#define R_LOG_INFO_SX(logCtx, marker, stream)   \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_INFO, marker, stream)
#define R_LOG_DEBUG_SX(logCtx, marker, stream)  \
    DSP_LOG_LOG_SX(logCtx, NActors::NLog::PRI_DEBUG, marker, stream)

} // NKikimr
