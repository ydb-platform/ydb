#include "log_settings.h"

#include <util/stream/str.h>

namespace NActors {
    namespace NLog {
        TSettings::TSettings(const TActorId& loggerActorId, const EComponent loggerComponent,
                             EComponent minVal, EComponent maxVal, EComponentToStringFunc func,
                             EPriority defPriority, EPriority defSamplingPriority,
                             ui32 defSamplingRate, ui64 timeThresholdMs, ui64 bufferSizeLimitBytes)
            : LoggerActorId(loggerActorId)
            , LoggerComponent(loggerComponent)
            , TimeThresholdMs(timeThresholdMs)
            , BufferSizeLimitBytes(bufferSizeLimitBytes)
            , AllowDrop(true)
            , ThrottleDelay(TDuration::MilliSeconds(100))
            , MinVal(0)
            , MaxVal(0)
            , Mask(0)
            , DefPriority(defPriority)
            , DefSamplingPriority(defSamplingPriority)
            , DefSamplingRate(defSamplingRate)
            , UseLocalTimestamps(false)
            , Format(PLAIN_FULL_FORMAT)
            , ShortHostName("")
            , ClusterName("")
            , TenantName("")
        {
            Append(minVal, maxVal, func);
        }

        TSettings::TSettings(const TActorId& loggerActorId, const EComponent loggerComponent,
                             EPriority defPriority, EPriority defSamplingPriority,
                             ui32 defSamplingRate, ui64 timeThresholdMs, ui64 bufferSizeLimitBytes)
            : LoggerActorId(loggerActorId)
            , LoggerComponent(loggerComponent)
            , TimeThresholdMs(timeThresholdMs)
            , BufferSizeLimitBytes(bufferSizeLimitBytes)
            , AllowDrop(true)
            , ThrottleDelay(TDuration::MilliSeconds(100))
            , MinVal(0)
            , MaxVal(0)
            , Mask(0)
            , DefPriority(defPriority)
            , DefSamplingPriority(defSamplingPriority)
            , DefSamplingRate(defSamplingRate)
            , UseLocalTimestamps(false)
            , Format(PLAIN_FULL_FORMAT)
            , ShortHostName("")
            , ClusterName("")
            , TenantName("")
        {
        }

        void TSettings::Append(EComponent minVal, EComponent maxVal, EComponentToStringFunc func) {
            Y_ABORT_UNLESS(minVal >= 0, "NLog::TSettings: minVal must be non-negative");
            Y_ABORT_UNLESS(maxVal > minVal, "NLog::TSettings: maxVal must be greater than minVal");

            // update bounds
            if (!MaxVal || minVal < MinVal) {
                MinVal = minVal;
            }

            if (!MaxVal || maxVal > MaxVal) {
                MaxVal = maxVal;

                // expand ComponentNames to the new bounds
                auto oldMask = Mask;
                Mask = PowerOf2Mask(MaxVal);

                TArrayHolder<TAtomic> oldComponentInfo(new TAtomic[Mask + 1]);
                ComponentInfo.Swap(oldComponentInfo);
                int startVal = oldMask ? oldMask + 1 : 0;
                for (int i = 0; i < startVal; i++) {
                    AtomicSet(ComponentInfo[i], AtomicGet(oldComponentInfo[i]));
                }

                TComponentSettings defSetting(DefPriority, DefSamplingPriority, DefSamplingRate);
                for (int i = startVal; i < Mask + 1; i++) {
                    AtomicSet(ComponentInfo[i], defSetting.Raw.Data);
                }

                ComponentNames.resize(Mask + 1);
            }

            // assign new names but validate if newly added members were not used before
            for (int i = minVal; i <= maxVal; i++) {
                Y_ABORT_UNLESS(!ComponentNames[i], "component name at %d already set: %s",
                    i, ComponentNames[i].data());
                ComponentNames[i] = func(i);
            }
        }

        int TSettings::SetLevelImpl(
            const TString& name, bool isSampling,
            EPriority priority, EComponent component, TString& explanation) {
            TString titleName(name);
            titleName.to_title();

            // check priority
            if (!IsValidPriority(priority)) {
                TStringStream str;
                str << "Invalid " << name;
                explanation = str.Str();
                return 1;
            }

            if (component == InvalidComponent) {
                for (int i = 0; i < Mask + 1; i++) {
                    TComponentSettings settings = AtomicGet(ComponentInfo[i]);
                    if (isSampling) {
                        settings.Raw.X.SamplingLevel = priority;
                    } else {
                        settings.Raw.X.Level = priority;
                    }
                    AtomicSet(ComponentInfo[i], settings.Raw.Data);
                }

                TStringStream str;

                str << titleName
                    << " for all components has been changed to "
                    << PriorityToString(EPrio(priority));
                explanation = str.Str();
                return 0;
            } else {
                if (!IsValidComponent(component)) {
                    explanation = "Invalid component";
                    return 1;
                }
                TComponentSettings settings = AtomicGet(ComponentInfo[component]);
                EPriority oldPriority;
                if (isSampling) {
                    oldPriority = (EPriority)settings.Raw.X.SamplingLevel;
                    settings.Raw.X.SamplingLevel = priority;
                } else {
                    oldPriority = (EPriority)settings.Raw.X.Level;
                    settings.Raw.X.Level = priority;
                }
                AtomicSet(ComponentInfo[component], settings.Raw.Data);
                TStringStream str;
                str << titleName << " for the component " << ComponentNames[component]
                    << " has been changed from " << PriorityToString(EPrio(oldPriority))
                    << " to " << PriorityToString(EPrio(priority));
                explanation = str.Str();
                return 0;
            }
        }

        int TSettings::SetLevel(EPriority priority, EComponent component, TString& explanation) {
            return SetLevelImpl("priority", false,
                                priority, component, explanation);
        }

        int TSettings::SetSamplingLevel(EPriority priority, EComponent component, TString& explanation) {
            return SetLevelImpl("sampling priority", true,
                                priority, component, explanation);
        }

        int TSettings::SetSamplingRate(ui32 sampling, EComponent component, TString& explanation) {
            if (component == InvalidComponent) {
                for (int i = 0; i < Mask + 1; i++) {
                    TComponentSettings settings = AtomicGet(ComponentInfo[i]);
                    settings.Raw.X.SamplingRate = sampling;
                    AtomicSet(ComponentInfo[i], settings.Raw.Data);
                }
                TStringStream str;
                str << "Sampling rate for all components has been changed to " << sampling;
                explanation = str.Str();
            } else {
                if (!IsValidComponent(component)) {
                    explanation = "Invalid component";
                    return 1;
                }
                TComponentSettings settings = AtomicGet(ComponentInfo[component]);
                ui32 oldSampling = settings.Raw.X.SamplingRate;
                settings.Raw.X.SamplingRate = sampling;
                AtomicSet(ComponentInfo[component], settings.Raw.Data);
                TStringStream str;
                str << "Sampling rate for the component " << ComponentNames[component]
                    << " has been changed from " << oldSampling
                    << " to " << sampling;
                explanation = str.Str();
            }
            return 0;
        }

        int TSettings::PowerOf2Mask(int val) {
            int mask = 1;
            while ((val & mask) != val) {
                mask <<= 1;
                mask |= 1;
            }
            return mask;
        }

        bool TSettings::IsValidPriority(EPriority priority) {
            return priority == PRI_EMERG || priority == PRI_ALERT ||
                   priority == PRI_CRIT || priority == PRI_ERROR ||
                   priority == PRI_WARN || priority == PRI_NOTICE ||
                   priority == PRI_INFO || priority == PRI_DEBUG || priority == PRI_TRACE;
        }

        bool TSettings::IsValidComponent(EComponent component) {
            return (MinVal <= component) && (component <= MaxVal) && !ComponentNames[component].empty();
        }

        void TSettings::SetAllowDrop(bool val) {
            AllowDrop = val;
        }

        void TSettings::SetThrottleDelay(TDuration value) {
            ThrottleDelay = value;
        }

        void TSettings::SetUseLocalTimestamps(bool value) {
            UseLocalTimestamps = value;
        }

        EComponent TSettings::FindComponent(const TStringBuf& componentName) const {
            if (componentName.empty())
                return InvalidComponent;

            for (EComponent component = MinVal; component <= MaxVal; ++component) {
                if (ComponentNames[component] == componentName)
                    return component;
            }

            return InvalidComponent;
        }

    }

}
