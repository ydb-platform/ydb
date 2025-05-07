#pragma once

#include "log_iface.h"
#include <util/generic/vector.h>
#include <util/digest/murmur.h>
#include <util/random/easy.h>

namespace NActors {
    namespace NLog {
        inline const char* PriorityToString(EPrio priority) {
            switch (priority) {
                case EPrio::Emerg:
                    return "EMERG";
                case EPrio::Alert:
                    return "ALERT";
                case EPrio::Crit:
                    return "CRIT";
                case EPrio::Error:
                    return "ERROR";
                case EPrio::Warn:
                    return "WARN";
                case EPrio::Notice:
                    return "NOTICE";
                case EPrio::Info:
                    return "INFO";
                case EPrio::Debug:
                    return "DEBUG";
                case EPrio::Trace:
                    return "TRACE";
                default:
                    return "UNKNOWN";
            }
        }

        // You can structure your program to have multiple logical components.
        // In this case you can set different log priorities for different
        // components. And you can change component's priority while system
        // is running. Suspect a component has a bug? Turn DEBUG priority level on
        // for this component.
        static const int InvalidComponent = -1;

        // Functions converts EComponent id to string
        using EComponentToStringFunc = std::function<const TString&(EComponent)>;

        // Log settings
        struct TComponentSettings {
            union {
                struct {
                    ui32 SamplingRate;
                    ui8 SamplingLevel;
                    ui8 Level;
                } X;

                ui64 Data = 0;
            } Raw;

            TComponentSettings(TAtomicBase data) {
                Raw.Data = (ui64)data;
            }

            TComponentSettings(ui8 level, ui8 samplingLevel, ui32 samplingRate) {
                Raw.X.Level = level;
                Raw.X.SamplingLevel = samplingLevel;
                Raw.X.SamplingRate = samplingRate;
            }
        };

        struct TSettings: public TThrRefBase {
        public:
            TActorId LoggerActorId;
            EComponent LoggerComponent;
            ui64 TimeThresholdMs;
            ui64 BufferSizeLimitBytes;
            bool AllowDrop;
            TDuration ThrottleDelay;
            TArrayHolder<TAtomic> ComponentInfo;
            TVector<TString> ComponentNames;
            EComponent MinVal;
            EComponent MaxVal;
            EComponent Mask;
            EPriority DefPriority;
            EPriority DefSamplingPriority;
            ui32 DefSamplingRate;
            bool UseLocalTimestamps;
            bool LogSourceLocation = true;

            enum ELogFormat {
                PLAIN_FULL_FORMAT,
                PLAIN_SHORT_FORMAT,
                JSON_FORMAT
            };
            ELogFormat Format;
            TString ShortHostName;
            TString ClusterName;
            TString TenantName;
            TString MessagePrefix;
            ui32 NodeId;

            // The best way to provide minVal, maxVal and func is to have
            // protobuf enumeration of components. In this case protoc
            // automatically generates YOURTYPE_MIN, YOURTYPE_MAX and
            // YOURTYPE_Name for you.
            TSettings(const TActorId& loggerActorId, const EComponent loggerComponent,
                      EComponent minVal, EComponent maxVal, EComponentToStringFunc func,
                      EPriority defPriority, EPriority defSamplingPriority = PRI_DEBUG,
                      ui32 defSamplingRate = 0, ui64 timeThresholdMs = 1000, ui64 bufferSizeLimitBytes = 1024 * 1024 * 300);

            TSettings(const TActorId& loggerActorId, const EComponent loggerComponent,
                      EPriority defPriority, EPriority defSamplingPriority = PRI_DEBUG,
                      ui32 defSamplingRate = 0, ui64 timeThresholdMs = 1000, ui64 bufferSizeLimitBytes = 1024 * 1024 * 300);

            void Append(EComponent minVal, EComponent maxVal, EComponentToStringFunc func);

            template <typename T>
            void Append(T minVal, T maxVal, const TString& (*func)(T)) {
                Append(
                    static_cast<EComponent>(minVal),
                    static_cast<EComponent>(maxVal),
                    [func](EComponent c) -> const TString& {
                        return func(static_cast<T>(c));
                    }
                );
            }

            inline bool Satisfies(EPriority priority, EComponent component, ui64 sampleBy = 0) const {
                // by using Mask we don't get outside of array boundaries
                TComponentSettings settings = GetComponentSettings(component);
                if (priority > settings.Raw.X.Level) {
                    if (priority > settings.Raw.X.SamplingLevel) {
                        return false; // priority > both levels ==> do not log
                    }
                    // priority <= sampling level ==> apply sampling
                    ui32 samplingRate = settings.Raw.X.SamplingRate;
                    if (samplingRate) {
                        ui32 samplingValue = sampleBy ? MurmurHash<ui32>((const char*)&sampleBy, sizeof(sampleBy))
                            : samplingRate != 1 ? RandomNumber<ui32>() : 0;
                        return (samplingValue % samplingRate == 0);
                    } else {
                        // sampling rate not set ==> do not log
                        return false;
                    }
                } else {
                    // priority <= log level ==> log
                    return true;
                }
            }

            inline TComponentSettings GetComponentSettings(EComponent component) const {
                Y_DEBUG_ABORT_UNLESS((component & Mask) == component);
                // by using Mask we don't get outside of array boundaries
                return TComponentSettings(AtomicGet(ComponentInfo[component & Mask]));
            }

            const char* ComponentName(EComponent component) const {
                Y_DEBUG_ABORT_UNLESS((component & Mask) == component);
                return ComponentNames[component & Mask].data();
            }

            int SetLevel(EPriority priority, EComponent component, TString& explanation);
            int SetSamplingLevel(EPriority priority, EComponent component, TString& explanation);
            int SetSamplingRate(ui32 sampling, EComponent component, TString& explanation);
            EComponent FindComponent(const TStringBuf& componentName) const;
            static int PowerOf2Mask(int val);
            static bool IsValidPriority(EPriority priority);
            bool IsValidComponent(EComponent component);
            void SetAllowDrop(bool val);
            void SetThrottleDelay(TDuration value);
            void SetUseLocalTimestamps(bool value);

        private:
            int SetLevelImpl(
                const TString& name, bool isSampling,
                EPriority priority, EComponent component, TString& explanation);
        };

    }

}
