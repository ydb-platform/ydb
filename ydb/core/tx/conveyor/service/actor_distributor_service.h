#pragma once
#include "actor_worker.h"
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NConveyor {

template<class T>
class TLoadQueue {
    using TLoad = i64;
    std::map<TLoad, std::set<T>> Load;
    std::map<T, TLoad> Items;

public:
    void Add(const T& item) {
        Load[0].emplace(item);
        Items[item] = 0;
    }

    const T& Top() {
        auto loadLevelIt = Load.begin();
        if (loadLevelIt == Load.end()) {
            ythrow yexception() << "Load queue is empty. Please add at least one element to load queue";
        }
        return *loadLevelIt->second.begin();
    }

    void ChangeLoad(const T& item, i64 delta) {
        if (delta == 0) {
            return;
        }
        auto it = Items.find(item);
        if (it == Items.end()) {
            return;
        }
        TLoad load = it->second;
        auto loadLevelIt = Load.find(load);
        loadLevelIt->second.erase(item);
        if (loadLevelIt->second.empty()) {
            Load.erase(loadLevelIt);
        }
        load += delta;
        it->second = load;
        Load[load].emplace(item);
    }
};

class TActorDistributor: public TActorBootstrapped<TActorDistributor> {
private:
    const TConfig Config;
    const TString ConveyorName = "common";
    std::map<TActorId, TActorId> InFlightActors;
    TLoadQueue<TActorId> Workers;

    void HandleMain(TEvExecution::TEvRegisterActor::TPtr& ev);
    void HandleMain(TEvExecution::TEvActorFinished::TPtr& ev);

public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecution::TEvRegisterActor, HandleMain);
            hFunc(TEvExecution::TEvActorFinished, HandleMain);
            default:
                AFL_ERROR(NKikimrServices::TX_CONVEYOR)("problem", "unexpected event for actor executor")("ev_type", ev->GetTypeName());
                break;
        }
    }

    TActorDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorCounters);

    void Bootstrap();
};

}
