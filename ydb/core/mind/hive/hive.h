#pragma once
#include <bitset>
#include <ranges>

#include <util/generic/queue.h>
#include <util/random/random.h>
#include <util/system/type_name.h>

#include <ydb/core/base/hive.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet/tablet_responsiveness_pinger.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/tablet/pipe_tracker.h>
#include <ydb/core/tablet/tablet_impl.h>

#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

#include <ydb/core/tablet/tablet_metrics.h>

#include <util/stream/format.h>

namespace NKikimr {
namespace NHive {

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::TTransactionContext;
using NTabletFlatExecutor::TExecutorCounters;

using TTabletId = ui64;
using TTabletCategoryId = ui64;
using TNodeId = ui32;
using TDataCenterId = TString;
using TFollowerId = ui32;
using TFollowerGroupId = ui32;
using TStorageGroupId = ui32;
using TFullTabletId = std::pair<TTabletId, TFollowerId>;
using TObjectId = ui64; // schema object id, used to organize tablets of the same schema object
using TOwnerId = ui64;
using TFullObjectId = std::pair<TOwnerId, TObjectId>;
using TResourceRawValues = std::tuple<i64, i64, i64, i64>; // CPU, Memory, Network, Counter
using TResourceNormalizedValues = std::tuple<double, double, double, double>;
using TOwnerIdxType = NScheme::TPairUi64Ui64;
using TSubActorId = ui64; // = LocalId part of TActorId

static constexpr std::size_t MAX_TABLET_CHANNELS = 256;

enum class ETabletState : ui64 {
    Unknown = 0,
    GroupAssignment = 50,
    StoppingInGroupAssignment = 98,
    Stopping = 99,
    Stopped = 100,
    ReadyToWork = 200,
    BlockStorage,   // blob storage block request for previous group of 0 channel
    Deleting,
};

TString ETabletStateName(ETabletState value);

enum class EFollowerStrategy : ui32 {
    Unknown,
    Backup,
    Read,
};

TString EFollowerStrategyName(EFollowerStrategy value);

// Note: the order in enum is used for priority
// Values are not required to be stable between versions
enum class EBalancerType {
    Manual,
    Emergency,
    SpreadNeighbours,
    Scatter,
    ScatterCounter,
    ScatterCPU,
    ScatterMemory,
    ScatterNetwork,
    Storage,

    Last = Storage,
};

constexpr std::size_t EBalancerTypeSize = static_cast<std::size_t>(EBalancerType::Last) + 1;

TString EBalancerTypeName(EBalancerType value);

enum class EResourceToBalance {
    ComputeResources,
    Counter,
    CPU,
    Memory,
    Network,
};

EResourceToBalance ToResourceToBalance(NMetrics::EResource resource);

struct ISubActor {
    const TInstant StartTime;

    virtual void Cleanup() = 0;

    virtual TString GetDescription() const {
        return TypeName(*this);
    }

    virtual TSubActorId GetId() const = 0;

    ISubActor() : StartTime(TActivationContext::Now()) {}
};


struct TCompleteNotifications {
    TVector<std::pair<THolder<IEventHandle>, TDuration>> Notifications;
    TActorId SelfID;

    void Reset(const TActorId& selfId) {
        Notifications.clear();
        SelfID = selfId;
    }

    void Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        Y_ABORT_UNLESS(!!SelfID);
        Notifications.emplace_back(new IEventHandle(recipient, SelfID, ev, flags, cookie), TDuration());
    }

    void Schedule(TDuration duration, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0) {
        Y_ABORT_UNLESS(!!SelfID);
        Notifications.emplace_back(new IEventHandle(SelfID, {}, ev, flags, cookie), duration);
    }

    size_t size() const {
        return Notifications.size();
    }

    void Send(const TActorContext& ctx) {
        for (auto& [notification, duration] : Notifications) {
            if (duration) {
                ctx.ExecutorThread.Schedule(duration, notification.Release());
            } else {
                ctx.ExecutorThread.Send(notification.Release());
            }
        }
        Notifications.clear();
    }
};

struct TCompleteActions {
    std::vector<std::unique_ptr<IActor>> Actors;
    std::vector<std::function<void()>> Callbacks;

    void Reset() {
        Actors.clear();
        Callbacks.clear();
    }

    void Register(IActor* actor) {
        Actors.emplace_back(actor);
    }

    void Callback(std::function<void()> callback) {
        Callbacks.emplace_back(std::move(callback));
    }

    void Run(const TActorContext& ctx) {
        for (auto& callback : Callbacks) {
            callback();
        }
        Callbacks.clear();
        for (auto& actor : Actors) {
            ctx.Register(actor.release());
        }
        Actors.clear();
    }
};

struct TSideEffects : TCompleteNotifications, TCompleteActions {
    void Reset(const TActorId& selfId) {
        TCompleteActions::Reset();
        TCompleteNotifications::Reset(selfId);
    }

    void Complete(const TActorContext& ctx) {
        TCompleteActions::Run(ctx);
        TCompleteNotifications::Send(ctx);
    }
};

TResourceNormalizedValues NormalizeRawValues(const TResourceRawValues& values, const TResourceRawValues& maximum);
NMetrics::EResource GetDominantResourceType(const TResourceRawValues& values, const TResourceRawValues& maximum);
NMetrics::EResource GetDominantResourceType(const TResourceNormalizedValues& normValues);

// We calculate resource standard deviation to eliminate pointless tablet moves
// Because counter is by default normalized by 1 000 000, a single tablet move
// might have a very small effect on overall deviation. We must not let numerical
// error be larger than the effect, so we use a more stable algorithm for computing the sum:
// https://en.wikipedia.org/wiki/Kahan_summation_algorithm
template<std::ranges::range TRange>
std::ranges::range_value_t<TRange> StableSum(const TRange& values) {
    using TValue = std::ranges::range_value_t<TRange>;
    TValue sum{};
    TValue correction{};
    for (const auto& x : values) {
        TValue y = x - correction;
        TValue tmp = sum + y;
        correction = (tmp - sum) - y;
        sum = tmp;
    }
    return sum;
}

template <typename... ResourceTypes>
inline std::tuple<ResourceTypes...> GetStDev(const TVector<std::tuple<ResourceTypes...>>& values) {
    std::tuple<ResourceTypes...> sum;
    if (values.empty())
        return sum;
    sum = StableSum(values);
    auto mean = sum / values.size();
    auto quadraticDev = [&] (const std::tuple<ResourceTypes...>& value) {
        auto diff = value - mean;
        return diff * diff;
    };
    sum = StableSum(values | std::views::transform(quadraticDev));
    auto div = sum / values.size();
    auto st_dev = sqrt(div);
    return tuple_cast<ResourceTypes...>::cast(st_dev);
}

extern const std::unordered_map<TTabletTypes::EType, TString> TABLET_TYPE_SHORT_NAMES;

extern const std::unordered_map<TString, TTabletTypes::EType> TABLET_TYPE_BY_SHORT_NAME;

class THive;

struct THiveSharedSettings {
    NKikimrConfig::THiveConfig CurrentConfig;

    NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy GetStorageBalanceStrategy() const {
        return CurrentConfig.GetStorageBalanceStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy GetStorageSelectStrategy() const {
        return CurrentConfig.GetStorageSelectStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveTabletBalanceStrategy GetTabletBalanceStrategy() const {
        return CurrentConfig.GetTabletBalanceStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveNodeBalanceStrategy GetNodeBalanceStrategy() const {
        return CurrentConfig.GetNodeBalanceStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveNodeSelectStrategy GetNodeSelectStrategy() const {
        return CurrentConfig.GetNodeSelectStrategy();
    }

    double GetStorageOvercommit() const {
        return CurrentConfig.GetStorageOvercommit();
    }

    bool GetStorageSafeMode() const {
        return CurrentConfig.GetStorageSafeMode();
    }

    TDuration GetStoragePoolFreshPeriod() const {
        return TDuration::MilliSeconds(CurrentConfig.GetStoragePoolFreshPeriod());
    }

    double GetMinGroupUsageToBalance() const {
        return CurrentConfig.GetMinGroupUsageToBalance();
    }
};

struct TDrainSettings {
    bool Persist = true;
    NKikimrHive::EDrainDownPolicy DownPolicy = NKikimrHive::EDrainDownPolicy::DRAIN_POLICY_KEEP_DOWN_UNTIL_RESTART;
    ui32 DrainInFlight = 0;
};

struct TBalancerSettings {
    EBalancerType Type = EBalancerType::Manual;
    int MaxMovements = 0;
    bool RecheckOnFinish = false;
    ui64 MaxInFlight = 1;
    std::vector<TNodeId> FilterNodeIds = {};
    EResourceToBalance ResourceToBalance = EResourceToBalance::ComputeResources;
    std::optional<TFullObjectId> FilterObjectId;
    TSubDomainKey FilterSubDomain;
};

struct TStorageBalancerSettings {
    ui64 NumReassigns;
    ui64 MaxInFlight = 1;
    TString StoragePool;
};

struct TBalancerStats {
    ui64 TotalRuns = 0;
    ui64 TotalMovements = 0;
    bool IsRunningNow = false;
    ui64 CurrentMovements = 0;
    ui64 CurrentMaxMovements = 0;
    TInstant LastRunTimestamp;
    ui64 LastRunMovements = 0;
};

struct TNodeFilter {
    TVector<TSubDomainKey> AllowedDomains;
    TVector<TNodeId> AllowedNodes;
    TVector<TDataCenterId> AllowedDataCenters;
    TSubDomainKey ObjectDomain;
    TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;

    const THive* Hive;

    explicit TNodeFilter(const THive& hive);

    TArrayRef<const TSubDomainKey> GetEffectiveAllowedDomains() const;

    bool IsAllowedDataCenter(TDataCenterId dc) const;
};

} // NHive
} // NKikimr

template <>
inline void Out<NKikimr::NHive::TCompleteNotifications>(IOutputStream& o, const NKikimr::NHive::TCompleteNotifications& n) {
    if (!n.Notifications.empty()) {
        o << "Notifications: ";
        for (auto it = n.Notifications.begin(); it != n.Notifications.end(); ++it) {
            if (it != n.Notifications.begin()) {
                o << ',';
            }
            o << Hex(it->first->Type) << " " << it->first.Get()->Recipient;
        }
    }
}

template <>
inline void Out<NKikimr::NHive::TCompleteActions>(IOutputStream& o, const NKikimr::NHive::TCompleteActions& n) {
    if (!n.Callbacks.empty()) {
        o << "Callbacks: " << n.Callbacks.size();
    }
    if (!n.Actors.empty()) {
        if (!n.Callbacks.empty()) {
            o << ' ';
        }
        o << "Actions: ";
        for (auto it = n.Actors.begin(); it != n.Actors.end(); ++it) {
            if (it != n.Actors.begin()) {
                o << '.';
            }
            o << TypeName(*(it->get()));
        }
    }
}

template <>
inline void Out<NKikimr::NHive::TSideEffects>(IOutputStream& o, const NKikimr::NHive::TSideEffects& e) {
    o << '{';
    o << static_cast<const NKikimr::NHive::TCompleteNotifications&>(e);
    if (!e.Notifications.empty() && !e.Actors.empty()) {
        o << ' ';
    }
    o << static_cast<const NKikimr::NHive::TCompleteActions&>(e);
    o << '}';
}
