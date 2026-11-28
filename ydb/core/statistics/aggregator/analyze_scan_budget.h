#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/statistics/events.h>

#include <util/generic/hash_set.h>

#include <algorithm>
#include <memory>

namespace NKikimr::NStat {

struct TAnalyzeScanLimits {
    ui32 Total;
    ui32 PerNode;

    TAnalyzeScanLimits(ui32 total, ui32 perNode)
        : Total(std::max(1u, total))
        , PerNode(std::max(1u, perNode))
    {}

    bool operator==(const TAnalyzeScanLimits&) const = default;
};

struct TEvAnalyzeScanBudget {
    // The budget sends its current limits to each table on Open and on reconfiguration.
    struct TEvConfigure : NActors::TEventLocal<TEvConfigure, TEvStatistics::EvAnalyzeScanBudgetConfigure> {
        TAnalyzeScanLimits Limits;

        TEvConfigure(ui32 totalLimit, ui32 perNodeLimit)
            : Limits(totalLimit, perNodeLimit)
        {}

        explicit TEvConfigure(TAnalyzeScanLimits limits)
            : Limits(limits)
        {}
    };

    // The sender of Open, Submit and Close is the table actor for this attempt.
    struct TEvOpen : NActors::TEventLocal<TEvOpen, TEvStatistics::EvAnalyzeScanBudgetOpen> {
        bool UserRequested;

        explicit TEvOpen(bool userRequested)
            : UserRequested(userRequested)
        {}
    };

    struct TEvSubmit : NActors::TEventLocal<TEvSubmit, TEvStatistics::EvAnalyzeScanBudgetSubmit> {
        ui32 NodeId; // Zero for a whole-table query which has no single target node.
        std::unique_ptr<NActors::IActor> Worker;

        TEvSubmit(ui32 nodeId, std::unique_ptr<NActors::IActor> worker)
            : NodeId(nodeId)
            , Worker(std::move(worker))
        {}
    };

    struct TEvClose : NActors::TEventLocal<TEvClose, TEvStatistics::EvAnalyzeScanBudgetClose> {};
    // Background actors whose paths are also queued by a user. This snapshot may arrive before Open.
    struct TEvSetUserDependencies : NActors::TEventLocal<TEvSetUserDependencies, TEvStatistics::EvAnalyzeScanBudgetSetUserDependencies> {
        THashSet<TActorId> Owners;

        explicit TEvSetUserDependencies(THashSet<TActorId> owners)
            : Owners(std::move(owners))
        {}
    };

    // Sent by each admitted worker to its registering actor on termination.
    struct TEvStopped : NActors::TEventLocal<TEvStopped, TEvStatistics::EvAnalyzeScanBudgetStopped> {};
};

NActors::IActor* CreateAnalyzeScanBudget(ui32 totalLimit, ui32 perNodeLimit);

} // namespace NKikimr::NStat
