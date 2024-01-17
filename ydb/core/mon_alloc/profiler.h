#pragma once

#include <ydb/core/protos/profiler.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>

namespace NActors {
    struct TEvProfiler {
        enum EEv {
            EvStart = EventSpaceBegin(TEvents::ES_PROFILER),
            EvStop,
            EvStartResult,
            EvStopResult,
            EvEnd
        };

        enum ESubscribes {
            SubConnected,
            SubDisconnected,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PROFILER), "ES_PROFILER event space is too small.");

        class TEvStart: public TEventPB<TEvStart, NActorsProfiler::TEvStart, EvStart> {
        public:
            TEvStart() {
            }

            TEvStart(ui64 cookie) {
                if (cookie) {
                    Record.SetCookie(cookie);
                }
            }

            ui64 Cookie() {
                return Record.HasCookie() ? Record.GetCookie() : 0;
            }
        };

        class TEvStartResult: public TEventPB<TEvStartResult, NActorsProfiler::TEvStartResult, EvStartResult> {
        public:
            TEvStartResult() {
            }

            TEvStartResult(ui64 cookie, bool isOk) {
                Record.SetCookie(cookie);
                Record.SetIsOk(isOk);
            }

            ui64 Cookie() {
                return Record.HasCookie() ? Record.GetCookie() : 0;
            }

            bool IsOk() {
                return Record.HasIsOk() ? Record.GetIsOk() : false;
            }
        };

        class TEvStop: public TEventPB<TEvStop, NActorsProfiler::TEvStop, EvStop> {
        public:
            TEvStop() {
            }

            TEvStop(ui64 cookie) {
                if (cookie) {
                    Record.SetCookie(cookie);
                }
            }

            ui64 Cookie() {
                return Record.HasCookie() ? Record.GetCookie() : 0;
            }
        };

        class TEvStopResult: public TEventPB<TEvStopResult, NActorsProfiler::TEvStopResult, EvStopResult> {
        public:
            TEvStopResult() {
            }

            TEvStopResult(ui64 cookie, TString profile, bool isOk) {
                Record.SetCookie(cookie);
                Record.SetProfile(profile);
                Record.SetIsOk(isOk);
            }

            ui64 Cookie() {
                return Record.HasCookie() ? Record.GetCookie() : 0;
            }

            TString Profile() {
                return Record.HasProfile() ? Record.GetProfile() : TString();
            }

            bool IsOk() {
                return Record.HasIsOk() ? Record.GetIsOk() : false;
            }
        };
    };

    struct IProfilerLogic {
        virtual ~IProfilerLogic() = default;
        virtual void Start() = 0;
        virtual void Stop(IOutputStream& out, size_t limit, bool forLog) = 0;
    };

    inline TActorId MakeProfilerID(ui32 nodeId) {
        char x[12] = {'p', 'r', 'o', 'f', 'i', 'l', 'e', 'r', 's', 'e', 'r', 'v'};
        return TActorId(nodeId, TStringBuf(x, 12));
    }

    IActor* CreateProfilerActor(
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        TString dir,
        std::unique_ptr<IProfilerLogic> profiler = nullptr);
}
