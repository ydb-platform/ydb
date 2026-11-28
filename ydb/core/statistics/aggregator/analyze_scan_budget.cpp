#include "analyze_scan_budget.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>

#include <algorithm>
#include <deque>
#include <list>

namespace NKikimr::NStat {
namespace {

class TAnalyzeScanBudget : public TActorBootstrapped<TAnalyzeScanBudget> {
    using TEv = TEvAnalyzeScanBudget;

    struct TScan {
        ui32 NodeId;
        std::unique_ptr<IActor> Worker;
    };

    struct TTable {
        TActorId Owner;
        bool UserRequested;
        std::deque<TScan> Pending;
    };

    struct TRunningScan {
        TActorId Owner;
        ui32 NodeId;
    };

    TAnalyzeScanLimits Limits;
    std::list<TTable> Tables;
    THashMap<TActorId, std::list<TTable>::iterator> ByOwner;
    THashMap<TActorId, TRunningScan> Running;
    THashMap<ui32, ui32> RunningByNode;
    THashSet<TActorId> BlockingBackgroundActors;

    bool StartNext(bool userPriority) {
        for (auto table = Tables.begin(); table != Tables.end(); ++table) {
            if ((table->UserRequested || BlockingBackgroundActors.contains(table->Owner)) != userPriority) {
                continue;
            }
            auto& pending = table->Pending;
            auto job = std::find_if(pending.begin(), pending.end(), [&](const TScan& scan) {
                const auto count = RunningByNode.FindPtr(scan.NodeId);
                return !count || *count < Limits.PerNode;
            });
            if (job == pending.end()) {
                continue;
            }
            const auto worker = Register(job->Worker.release(), TMailboxType::HTSwap, AppData()->BatchPoolId);
            Running.emplace(worker, TRunningScan{table->Owner, job->NodeId});
            if (job->NodeId) {
                ++RunningByNode[job->NodeId];
            }
            pending.erase(job);
            // Give the other eligible tables of the same priority the next slot.
            Tables.splice(Tables.end(), Tables, table);
            return true;
        }
        return false;
    }

    void Dispatch() {
        while (Running.size() < Limits.Total) {
            if (!StartNext(true) && !StartNext(false)) {
                break;
            }
        }
    }

    void Handle(TEv::TEvConfigure::TPtr& ev) {
        if (Limits == ev->Get()->Limits) {
            return;
        }
        Limits = ev->Get()->Limits;
        for (const auto& table : Tables) {
            Send(table.Owner, new TEv::TEvConfigure(Limits));
        }
        Dispatch();
    }

    void Handle(TEv::TEvOpen::TPtr& ev) {
        if (!ByOwner.contains(ev->Sender)) {
            auto table = Tables.insert(Tables.end(), TTable{ev->Sender, ev->Get()->UserRequested, {}});
            ByOwner.emplace(ev->Sender, table);
            Send(ev->Sender, new TEv::TEvConfigure(Limits));
        }
    }

    void Handle(TEv::TEvSetUserDependencies::TPtr& ev) {
        BlockingBackgroundActors = std::move(ev->Get()->Owners);
    }

    void Handle(TEv::TEvSubmit::TPtr& ev) {
        auto table = ByOwner.find(ev->Sender);
        if (table == ByOwner.end()) {
            return;
        }
        table->second->Pending.push_back(TScan{ev->Get()->NodeId, std::move(ev->Get()->Worker)});
        Dispatch();
    }

    void Handle(TEv::TEvStopped::TPtr& ev) {
        auto worker = Running.find(ev->Sender);
        if (worker == Running.end()) {
            return;
        }
        if (ui32 nodeId = worker->second.NodeId) {
            auto count = RunningByNode.find(nodeId);
            if (--count->second == 0) {
                RunningByNode.erase(count);
            }
        }
        Running.erase(worker);
        Dispatch();
    }

    void Handle(TEv::TEvClose::TPtr& ev) {
        BlockingBackgroundActors.erase(ev->Sender);
        auto table = ByOwner.find(ev->Sender);
        if (table == ByOwner.end()) {
            return;
        }
        Tables.erase(table->second);
        ByOwner.erase(table);
        // Retain the slots for cancelled workers until their stop events arrive.
        for (const auto& [worker, scan] : Running) {
            if (scan.Owner == ev->Sender) {
                Send(worker, new TEvents::TEvPoison());
            }
        }
    }

    void PassAway() override {
        for (const auto& [worker, scan] : Running) {
            Send(worker, new TEvents::TEvPoison());
        }
        TActorBootstrapped::PassAway();
    }

public:
    TAnalyzeScanBudget(ui32 totalLimit, ui32 perNodeLimit)
        : Limits(totalLimit, perNodeLimit)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEv::TEvConfigure, Handle)
        hFunc(TEv::TEvOpen, Handle)
        hFunc(TEv::TEvSetUserDependencies, Handle)
        hFunc(TEv::TEvSubmit, Handle)
        hFunc(TEv::TEvStopped, Handle)
        hFunc(TEv::TEvClose, Handle)
        sFunc(TEvents::TEvPoison, PassAway)
    )
};

} // namespace

IActor* CreateAnalyzeScanBudget(ui32 totalLimit, ui32 perNodeLimit) {
    return new TAnalyzeScanBudget(totalLimit, perNodeLimit);
}

} // namespace NKikimr::NStat
