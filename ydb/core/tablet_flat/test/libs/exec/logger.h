#pragma once

#include "world.h"

#include <ydb/core/tablet_flat/util_basics.h>
#include <ydb/core/tablet_flat/util_fmt_line.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log_iface.h>

namespace NKikimr {
namespace NFake {

    struct TSink : public TAtomicRefCount<TSink> {

        using EComp = NActors::NLog::EComponent;

        TSink(TInstant start, TVector<TString> names)
            : Start(start)
            , Names(std::move(names))
        {

        }

        void Put(TInstant stamp, ui32 level, EComp comp, TArrayRef<const char> line) noexcept
        {
            Y_ABORT_UNLESS(line.size() < 8192 * 16, "Too large log line");

            static const char scaleMajor[] = "^^*CEWNIDT.";
            static const char scaleMinor[] = "0123456789.";

            ++Stats_[Min(ui32(level) >> 8, ui32(10))];

            const auto left = (stamp - Start).SecondsFloat();
            const auto major = scaleMajor[Min(ui32(level >> 8), ui32(10))];
            const auto minor = level & 0xff;

            TString cname;

            if (ui32(comp) < Names.size() && Names[comp]) {
                cname = Names[comp];
            } else {
                cname = Sprintf("?COMP_%04u", comp);
            }

            auto raw = Sprintf("%09.3f %c%c| %s: %.*s\n",
                        left,
                        major,
                        minor ? scaleMinor[Min(minor, ui32(10))] : major,
                        cname.data(),
                        int(line.size()), line.data());

            while (true) {
                try {
                    Cerr.Write(raw.data(), raw.size());
                    break;
                } catch (TSystemError) {
                    /* interrupted syscall */
                }
            }
        }

        TArrayRef<const ui64> Stats() const noexcept
        {
            return Stats_;
        }

    public:
        const TInstant Start = TInstant::Max();
        const TVector<TString> Names;   /* { componnet -> name } */

    private:
        std::array<ui64, 11> Stats_{ };
    };


    class TLogEnv : private NUtil::ILogged, public NUtil::ILogger {
    public:
        using EPrio = ::NActors::NLog::EPriority;
        using ELnLev = NUtil::ELnLev;

        TLogEnv(ITimeProvider *time, ELnLev level,TIntrusivePtr<TSink> sink)
            : Time(time)
            , Level(level)
            , Sink(std::move(sink))
        {

        }

        NUtil::TLogLn Log(ELnLev prio) const noexcept override
        {
            return { prio <= Level ? this : nullptr, prio };
        }

    private:
        void LogLn(ELnLev prio, const TString &line) const noexcept override
        {
            const auto comp = NKikimrServices::FAKE_ENV;

            /* Usage of time provider will be possible on complete migration
                of all logging clients to provider aware code.
             */

            auto stamp = false ? Time->Now() : TInstant::Now();

            Sink->Put(stamp, ui32(prio), comp, line);
        }

    private:
        ITimeProvider * const Time = nullptr;
        const ELnLev Level = ELnLev::Warn;
        const TIntrusivePtr<TSink> Sink;
    };


    class TLogFwd : public ::NActors::IActorCallback {
    public:
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;

        TLogFwd(TIntrusivePtr<TSink> sink)
            : ::NActors::IActorCallback(static_cast<TReceiveFunc>(&TLogFwd::Inbox), IActor::EActivityType::LOG_ACTOR)
            , Sink(std::move(sink))
        {
        }

    private:
        void Inbox(TEventHandlePtr &eh)
        {
            if (auto *ev = eh->CastAsLocal<NActors::NLog::TEvLog>()) {
                Sink->Put(ev->Stamp, ev->Level.Raw, ev->Component, ev->Line);

                if (ev->Level.IsUrgentAbortion())
                    Send(TWorld::Where(EPath::Root), new NFake::TEvTerm);

            } else {
                Y_ABORT("Test runtime env logger got an unknown event");
            }
        }

    private:
        TIntrusivePtr<TSink> Sink;
    };

}
}
