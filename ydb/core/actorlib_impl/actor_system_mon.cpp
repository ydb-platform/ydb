#include "actor_system_mon.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/mon/crossref.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>

using namespace NActors;

namespace NKikimr {

class TActorSystemMonQueryProcessor : public TActorBootstrapped<TActorSystemMonQueryProcessor> {
    IHarmonizer* Harmonizer;
    NMon::TEvHttpInfo::TPtr Ev;

public:
    TActorSystemMonQueryProcessor(IHarmonizer* harmonizer, NMon::TEvHttpInfo::TPtr ev)
        : Harmonizer(harmonizer)
        , Ev(ev)
    {}

    void TryToReadHistory() {
        TStringStream str;
        bool success = Harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            i64 size = history.size();
            i64 begin = size - 10;
            if (size < 10) {
                begin = 0;
            }

            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Harmonizer";
                    }
                    DIV_CLASS("panel-body") {
                        auto stats = Harmonizer->GetStats();
                        DIV() {
                            str << "Harmonizer stats: " << stats.ToString() << "<br/>";
                            str << "Maximum CPU usage: " << stats.MaxUsedCpu << "<br/>";
                            str << "Minimum CPU usage: " << stats.MinUsedCpu << "<br/>";
                            str << "Maximum CPU time: " << stats.MaxElapsedCpu << "<br/>";
                            str << "Minimum CPU time: " << stats.MinElapsedCpu << "<br/>";
                            str << "Average awakening time (us): " << stats.AvgAwakeningTimeUs << "<br/>";
                            str << "Average waking up time (us): " << stats.AvgWakingUpTimeUs;
                        }
                    }
                }

                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table table-bordered table-condensed") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "Iteration"; }
                                TABLEH() { str << "Timestamp"; }
                                TABLEH() { str << "Budget"; }
                                TABLEH() { str << "Lost CPU"; }
                                TABLEH() { str << "Free shared CPU"; }
                            }
                        }
                        TABLEBODY() {
                            for (i64 idx = size - 1; idx >= begin; --idx) {
                                TABLER() {
                                    TABLED() { str << history[idx].Iteration; }
                                    TABLED() { str << history[idx].Ts; }
                                    TABLED() { str << history[idx].Budget; }
                                    TABLED() { str << history[idx].LostCpu; }
                                    TABLED() { str << history[idx].FreeSharedCpu; }
                                }
                            }
                        }
                    }
                }
            }
        });
        if (!success) {
            Schedule(TDuration::MicroSeconds(100), new TEvents::TEvWakeup());
        }

        Send(Ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
        PassAway();
    }

    void Bootstrap() {
        Become(&TThis::StateReadHistory);
        TryToReadHistory();
    }

    STATEFN(StateReadHistory) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Poison, PassAway);
            sFunc(TEvents::TEvWakeup, TryToReadHistory);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TActorSystemMonActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TActorSystemMonActor : public TActorBootstrapped<TActorSystemMonActor> {
    IHarmonizer* Harmonizer = nullptr;

public:
    TActorSystemMonActor()
    {}

    void Bootstrap() {
        Harmonizer = TActivationContext::ActorSystem()->GetHarmonizer();
        Y_ABORT_UNLESS(Harmonizer);
        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(TMon::TRegisterActorPageFields{
                .Title = "Actor System",
                .RelPath = "actor_system",
                .ActorSystem = TActivationContext::ActorSystem(),
                .Index = actorsMonPage, 
                .PreTag = false, 
                .ActorId = SelfId(),
                .MonServiceName = "actor_system_mon"
            });
        }

        Become(&TThis::StateOnline);
    }

    STATEFN(StateOnline) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
            hFunc(NMon::TEvHttpInfo, Handle);
        default:
            break;
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        if (!Harmonizer) {
            Send(ev->Sender, new NMon::TEvHttpInfoRes("Actor System Harmonizer is not initialized"));
            return;
        }
        Register(new TActorSystemMonQueryProcessor(Harmonizer, ev));
    }
};

TActorId MakeActorSystemMonId() {
    return TActorId(0, TStringBuf("ActorSystemMon", 12));
}

IActor* CreateActorSystemMon() {
    return new TActorSystemMonActor();
}

} // NKikimr
