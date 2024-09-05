#pragma once

#include "events.h"
#include "event_local.h"

namespace NActors {
    namespace NLog {
        using EComponent = int;

        enum EPriority : ui16 { // migrate it to EPrio whenever possible
            PRI_EMERG /* "EMERG" */,
            PRI_ALERT /* "ALERT" */,
            PRI_CRIT /* "CRIT" */,
            PRI_ERROR /* "ERROR" */,
            PRI_WARN /* "WARN" */,
            PRI_NOTICE /* "NOTICE" */,
            PRI_INFO /* "INFO" */,
            PRI_DEBUG /* "DEBUG" */,
            PRI_TRACE /* "TRACE" */
        };

        enum class EPrio : ui16 {
            Emerg = 0,
            Alert = 1,
            Crit = 2,
            Error = 3,
            Warn = 4,
            Notice = 5,
            Info = 6,
            Debug = 7,
            Trace = 8,
        };

        struct TLevel {
            TLevel(ui32 raw)
                : Raw(raw)
            {
            }

            TLevel(EPrio prio)
                : Raw((ui16(prio) + 1) << 8)
            {
            }

            EPrio ToPrio() const noexcept {
                const auto major = Raw >> 8;

                return major > 0 ? EPrio(major - 1) : EPrio::Emerg;
            }

            bool IsUrgentAbortion() const noexcept {
                return (Raw >> 8) == 0;
            }

            /* Generalized monotonic level value composed with major and minor
            levels. Minor is used for verbosity within major, basic EPrio
            mapped to (EPrio + 1, 0) and Major = 0 is reserved as special
            space with meaning like EPrio::Emerg but with extened actions.
            Thus logger should map Major = 0 to EPrio::Emerg if it have no
            idea how to handle special emergency actions.
         */

            ui32 Raw = 0; // ((ui16(EPrio) + 1) << 8) | ui8(minor)
        };

        enum class EEv {
            Log = EventSpaceBegin(TEvents::ES_LOGGER),
            LevelReq,
            LevelResp,
            Ignored,
            Buffer,
            End
        };

        static_assert(int(EEv::End) < EventSpaceEnd(TEvents::ES_LOGGER), "");

        struct TEvLogBufferMainListTag {};
        struct TEvLogBufferLevelListTag {};

        class TEvLog
            : public TEventLocal<TEvLog, int(EEv::Log)>
            , public TIntrusiveListItem<TEvLog, TEvLogBufferMainListTag>
            , public TIntrusiveListItem<TEvLog, TEvLogBufferLevelListTag>
        {
        public:
            TEvLog(TInstant stamp, TLevel level, EComponent comp, const TString &line, bool json = false)
                : Stamp(stamp)
                , Level(level)
                , Component(comp)
                , Line(line)
                , Json(json)
            {
            }

            TEvLog(TInstant stamp, TLevel level, EComponent comp, TString &&line, bool json = false)
                : Stamp(stamp)
                , Level(level)
                , Component(comp)
                , Line(std::move(line))
                , Json(json)
            {
            }

            TEvLog(EPriority prio, EComponent comp, TString line, TInstant time = TInstant::Now())
                : Stamp(time)
                , Level(EPrio(prio))
                , Component(comp)
                , Line(std::move(line))
                , Json(false)
            {
            }

            TEvLog(EPriority prio, EComponent comp, TString line, bool json, TInstant time = TInstant::Now())
                : Stamp(time)
                , Level(EPrio(prio))
                , Component(comp)
                , Line(std::move(line))
                , Json(json)
            {
            }

            const TInstant Stamp = TInstant::Max();
            const TLevel Level;
            const EComponent Component = 0;
            TString Line;
            const bool Json;
        };

    }
}
