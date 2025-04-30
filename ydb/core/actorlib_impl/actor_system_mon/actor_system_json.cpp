#include "actor_system_mon.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/mon/crossref.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_writer.h>

using namespace NActors;

namespace NKikimr::NActorSystemMon {

enum class ELevel {
    Iteration,
    Pool,
    Thread,
};

struct TLastWindowIteration {
    ui64 LastIterationCount;
};

struct TLastWindowTime {
    ui64 LastWindowTs;
};

struct TRangeWindowIteration {
    ui64 IterationCount;
    ui64 IterationStart;
};

struct TRangeWindowTime {
    ui64 WindowTs;
    ui64 WindowStart;
};

struct TQueryCfg {
    ELevel Level;
    std::variant<TLastWindowIteration, TRangeWindowIteration, TLastWindowTime, TRangeWindowTime> Window;
};


void GenerateJson(const TIterableDoubleRange<THarmonizerIterationState>& history, TStringStream& response, TQueryCfg& cfg) {
    NJson::TJsonWriter writer(&response, false);

    writer.OpenMap();
    writer.WriteKey("history");
    writer.OpenArray();

    i64 begin = 0;
    i64 end = history.size();

    std::visit([&](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, TLastWindowIteration>) {
            if (arg.LastIterationCount > history.size()) {
                begin = 0;
            } else {
                begin = history.size() - arg.LastIterationCount;
            }
        } else if constexpr (std::is_same_v<T, TLastWindowTime>) {
            ui64 currentTs = GetCycleCountFast();
            auto it = std::upper_bound(history.begin(), history.end(), currentTs - arg.LastWindowTs, [](ui64 ts, const THarmonizerIterationState& state) {
                return ts <= state.Ts;
            });
            begin = it - history.begin();
        } else if constexpr (std::is_same_v<T, TRangeWindowIteration>) {
            auto itBegin = std::upper_bound(history.begin(), history.end(), arg.IterationStart, [](ui64 iteration, const THarmonizerIterationState& state) {
                return iteration <= state.Iteration;
            });
            begin = itBegin - history.begin();
            auto itEnd = std::upper_bound(itBegin, history.end(), arg.IterationStart + arg.IterationCount, [](ui64 iteration, const THarmonizerIterationState& state) {
                return iteration <= state.Iteration;
            });
            end = itEnd - history.begin();
        } else if constexpr (std::is_same_v<T, TRangeWindowTime>) {
            auto itBegin = std::upper_bound(history.begin(), history.end(), arg.WindowStart, [](ui64 ts, const THarmonizerIterationState& state) {
                return ts <= state.Ts;
            });
            begin = itBegin - history.begin();
            auto itEnd = std::upper_bound(itBegin, history.end(), arg.WindowStart + arg.WindowTs, [](ui64 ts, const THarmonizerIterationState& state) {
                return ts <= state.Ts;
            });
            end = itEnd - history.begin();
        }
    }, cfg.Window);

    if (begin >= end || begin < 0 || end < 0 || begin >= static_cast<i64>(history.size()) || end > static_cast<i64>(history.size())) {
        writer.CloseArray();
        writer.CloseMap();
        return;
    }

    for (i64 idx = begin; idx < end; ++idx) {
        writer.OpenMap();
        
        // Базовые поля состояния
        writer.WriteKey("iteration");
        writer.Write(history[idx].Iteration);
        writer.WriteKey("timestamp");
        writer.Write(history[idx].Ts);
        writer.WriteKey("budget");
        writer.Write(history[idx].Budget);
        writer.WriteKey("lostCpu");
        writer.Write(history[idx].LostCpu);
        writer.WriteKey("freeSharedCpu");
        writer.Write(history[idx].FreeSharedCpu);
        
        // Информация о пулах
        if (cfg.Level == ELevel::Pool || cfg.Level == ELevel::Thread) {
            writer.WriteKey("pools");
            writer.OpenArray();
            for (size_t poolIdx = 0; poolIdx < history[idx].Pools.size(); ++poolIdx) {
                const auto& pool = history[idx].Pools[poolIdx];
                writer.OpenMap();
                
                // Информация о пуле
                if (pool.PersistentState) {
                    writer.WriteKey("name");
                    writer.Write(pool.PersistentState->Name);
                    writer.WriteKey("defaultThreadCount");
                    writer.Write(pool.PersistentState->DefaultThreadCount);
                    writer.WriteKey("minThreadCount");
                    writer.Write(pool.PersistentState->MinThreadCount);
                    writer.WriteKey("maxThreadCount");
                    writer.Write(pool.PersistentState->MaxThreadCount);
                    writer.WriteKey("priority");
                    writer.Write(pool.PersistentState->Priority);
                    writer.WriteKey("maxLocalQueueSize");
                    writer.Write(pool.PersistentState->MaxLocalQueueSize);
                    writer.WriteKey("minLocalQueueSize");
                    writer.Write(pool.PersistentState->MinLocalQueueSize);
                } else {
                    writer.WriteKey("name");
                    writer.Write("Unknown");
                }
                
                writer.WriteKey("avgPingUs");
                writer.Write(pool.AvgPingUs);
                writer.WriteKey("avgPingUsWithSmallWindow");
                writer.Write(pool.AvgPingUsWithSmallWindow);
                writer.WriteKey("maxAvgPingUs");
                writer.Write(pool.MaxAvgPingUs);
                
                writer.WriteKey("operation");
                writer.Write(pool.Operation.ToString());
                
                writer.WriteKey("potentialMaxThreadCount");
                writer.Write(pool.PotentialMaxThreadCount);
                writer.WriteKey("currentThreadCount");
                writer.Write(pool.CurrentThreadCount);
                writer.WriteKey("localQueueSize");
                writer.Write(pool.LocalQueueSize);
                writer.WriteKey("isNeedy");
                writer.Write(pool.IsNeedy);
                writer.WriteKey("isStarved");
                writer.Write(pool.IsStarved);
                writer.WriteKey("isHoggish");
                writer.Write(pool.IsHoggish);
                

                if (cfg.Level == ELevel::Thread) {
                    // Информация о потоках в пуле
                    writer.WriteKey("threads");
                    writer.OpenArray();
                    for (size_t threadIdx = 0; threadIdx < pool.Threads.size(); ++threadIdx) {
                        const auto& thread = pool.Threads[threadIdx];
                        writer.OpenMap();
                        
                        writer.WriteKey("usedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(thread.UsedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(thread.UsedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.WriteKey("elapsedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(thread.ElapsedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(thread.ElapsedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.WriteKey("parkedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(thread.ParkedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(thread.ParkedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.CloseMap(); // thread
                    }
                    writer.CloseArray(); // threads
                }
                
                writer.CloseMap(); // pool
            }
            writer.CloseArray(); // pools
        
            // Информация о разделяемом пуле
            writer.WriteKey("shared");
            writer.OpenMap();
            
            if (cfg.Level == ELevel::Thread) {
                writer.WriteKey("threads");
                writer.OpenArray();
                for (size_t threadIdx = 0; threadIdx < history[idx].Shared.Threads.size(); ++threadIdx) {
                    const auto& sharedThread = history[idx].Shared.Threads[threadIdx];
                    writer.OpenMap();
                    
                    writer.WriteKey("byPool");
                    writer.OpenArray();
                    for (size_t poolIdx = 0; poolIdx < sharedThread.ByPool.size(); ++poolIdx) {
                        const auto& byPool = sharedThread.ByPool[poolIdx];
                        writer.OpenMap();
                        
                        writer.WriteKey("usedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(byPool.UsedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(byPool.UsedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.WriteKey("elapsedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(byPool.ElapsedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(byPool.ElapsedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.WriteKey("parkedCpu");
                        writer.OpenMap();
                        writer.WriteKey("cpu");
                        writer.Write(byPool.ParkedCpu.Cpu);
                        writer.WriteKey("lastSecondCpu");
                        writer.Write(byPool.ParkedCpu.LastSecondCpu);
                        writer.CloseMap();
                        
                        writer.CloseMap(); // byPool
                    }
                    writer.CloseArray(); // byPool

                    writer.CloseMap(); // sharedThread
                }
                writer.CloseArray(); // threads
            }
            
            writer.CloseMap(); // shared
        }
        
        writer.CloseMap(); // history item
    }
    writer.CloseArray();
    writer.CloseMap();           
}


class TActorSystemMonQueryProcessor : public TActorBootstrapped<TActorSystemMonQueryProcessor> {
    IHarmonizer* Harmonizer;
    NMon::TEvHttpInfo::TPtr Ev;

public:
    TActorSystemMonQueryProcessor(IHarmonizer* harmonizer, NMon::TEvHttpInfo::TPtr ev)
        : Harmonizer(harmonizer)
        , Ev(ev)
    {}

    void TryToReadHistory() {
        const TCgiParameters& cgi = Ev->Get()->Request.GetParams();

        TStringStream str;
        bool success = Harmonizer->InvokeReadHistory([&](const TIterableDoubleRange<THarmonizerIterationState>& history) {
            TQueryCfg cfg;
            cfg.Level = ELevel::Iteration;
            if (cgi.Has("level")) {
                if (cgi.Get("level") == "pool") {
                    cfg.Level = ELevel::Pool;
                } else if (cgi.Get("level") == "thread") {
                    cfg.Level = ELevel::Thread;
                }
            }

            cfg.Window = TLastWindowIteration{10};
            if (cgi.Has("last_iteration")) {
                cfg.Window = TLastWindowIteration{FromString<ui64>(cgi.Get("last_iteration"))};
            } else if (cgi.Has("last_window_ts")) {
                cfg.Window = TLastWindowTime{FromString<ui64>(cgi.Get("last_window_ts"))};
            } else if (cgi.Has("window_iteration_start")) {
                ui64 start = FromString<ui64>(cgi.Get("window_iteration_start"));
                ui64 count = 10;
                if (cgi.Has("window_iteration_count")) {
                    count = FromString<ui64>(cgi.Get("window_iteration_count"));
                }
                cfg.Window = TRangeWindowIteration{count, start};
            } else if (cgi.Has("window_ts_start")) {
                ui64 start = FromString<ui64>(cgi.Get("window_ts_start"));
                ui64 count = Us2Ts(10'000'000);
                if (cgi.Has("window_ts_count")) {
                    count = FromString<ui64>(cgi.Get("window_ts_count"));
                }
                cfg.Window = TRangeWindowTime{count, start};
            }
            GenerateJson(history, str, cfg);
        });
        
        if (!success) {
            Schedule(TDuration::MicroSeconds(100), new TEvents::TEvWakeup());
            return;
        }

        Send(Ev->Sender, new NMon::TEvHttpInfoRes(TStringBuilder() << NMonitoring::HTTPOKJSON << str.Str(), 0, NMon::TEvHttpInfoRes::Custom));

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

} // NKikimr::NActorSystemMon
