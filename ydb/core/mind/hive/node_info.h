#pragma once

#include "hive.h"
#include "tablet_info.h"

namespace NKikimr {
namespace NHive {

struct TTabletInfo;

struct TNodeInfo {
    enum class EVolatileState {
        Unknown,
        Disconnected,
        Connecting,
        Connected,
        Disconnecting,
    };

    static TString EVolatileStateName(EVolatileState value) {
        switch(value) {
        case EVolatileState::Unknown: return "Unknown";
        case EVolatileState::Disconnected: return "Disconnected";
        case EVolatileState::Connecting: return "Connecting";
        case EVolatileState::Connected: return "Connected";
        case EVolatileState::Disconnecting: return "Disconnecting";
        default: return Sprintf("%d", static_cast<int>(value));
        }
    }

protected:
    EVolatileState VolatileState;

public:
    static const ui64 MAX_TABLET_COUNT_DEFAULT_VALUE;

    struct TTabletAvailabilityInfo {
        NKikimrLocal::TTabletAvailability FromLocal;
        ui64 EffectiveMaxCount;
        bool IsSet;

        TTabletAvailabilityInfo(const NKikimrLocal::TTabletAvailability& availability) : FromLocal(availability)
                                                                                       , EffectiveMaxCount(availability.GetMaxCount())
                                                                                       , IsSet(EffectiveMaxCount != MAX_TABLET_COUNT_DEFAULT_VALUE)
        {
        }

        void UpdateRestriction(ui64 restriction) {
            EffectiveMaxCount = std::min(FromLocal.GetMaxCount(), restriction);
            IsSet = true;
        }

        void RemoveRestriction() {
            // We set IsSet to true if we are removing a restriction that does not exist anyway
            // This way, it takes priority over DefaultTabletCount from HiveConfig
            IsSet = (EffectiveMaxCount == FromLocal.GetMaxCount() || FromLocal.GetMaxCount() != MAX_TABLET_COUNT_DEFAULT_VALUE);
            EffectiveMaxCount = FromLocal.GetMaxCount();
        }
    };

    THive& Hive;
    TNodeId Id;
    TActorId Local;
    bool Down;
    bool Freeze;
    bool Drain;
    bool BecomeUpOnRestart = false;
    TVector<TActorId> DrainInitiators;
    TDrainSettings DrainSettings;
    std::unordered_map<TTabletInfo::EVolatileState, std::unordered_set<TTabletInfo*>> Tablets;
    std::unordered_map<TTabletTypes::EType, std::unordered_set<TTabletInfo*>> TabletsRunningByType;
    std::unordered_map<TFullObjectId, std::unordered_set<TTabletInfo*>> TabletsOfObject;
    std::vector<TFullTabletId> FrozenTablets;
    TResourceRawValues ResourceValues; // accumulated resources from tablet metrics
    TResourceRawValues ResourceTotalValues; // actual used resources from the node (should be greater or equal one above)
    NMetrics::TAverageValue<TResourceRawValues, 20> AveragedResourceTotalValues;
    double NodeTotalUsage = 0;
    NMetrics::TFastRiseAverageValue<double, 20> AveragedNodeTotalUsage;
    TResourceRawValues ResourceMaximumValues;
    TInstant StartTime;
    TNodeLocation Location;
    bool LocationAcquired;
    std::unordered_map<TTabletTypes::EType, TTabletAvailabilityInfo> TabletAvailability;
    std::unordered_map<TTabletTypes::EType, ui64> TabletAvailabilityRestrictions;
    TVector<TSubDomainKey> ServicedDomains;
    TVector<TSubDomainKey> LastSeenServicedDomains;
    TVector<TActorId> PipeServers;
    THashSet<TLeaderTabletInfo*> LockedTablets;
    mutable TInstant LastResourceChangeReaction;
    NKikimrHive::TNodeStatistics Statistics;
    bool DeletionScheduled = false;
    TString Name;

    TNodeInfo(TNodeId nodeId, THive& hive);
    TNodeInfo(const TNodeInfo&) = delete;
    TNodeInfo(TNodeInfo&&) = delete;
    TNodeInfo& operator =(const TNodeInfo&) = delete;
    TNodeInfo& operator =(TNodeInfo&&) = delete;

    TString GetLogPrefix() const;

    EVolatileState GetVolatileState() const {
        return VolatileState;
    }

    void ChangeVolatileState(EVolatileState state);
    bool OnTabletChangeVolatileState(TTabletInfo* tablet, TTabletInfo::EVolatileState newState);
    void UpdateResourceValues(const TTabletInfo* tablet, const NKikimrTabletBase::TMetrics& before, const NKikimrTabletBase::TMetrics& after);

    ui32 GetTabletsScheduled() const {
        auto it = Tablets.find(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_STARTING);
        if (it != Tablets.end())
            return it->second.size();
        return 0;
    }

    ui32 GetTabletsTotal() const {
        ui32 totalSize = 0;
        for (const auto& t : Tablets) {
            totalSize += t.second.size();
        }
        return totalSize;
    }

    ui32 GetTabletsRunning() const {
        auto it = Tablets.find(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING);
        if (it != Tablets.end())
            return it->second.size();
        return 0;
    }

    ui32 GetTabletNeighboursCount(const TTabletInfo& tablet) const {
        auto it = TabletsOfObject.find(tablet.GetObjectId());
        if (it != TabletsOfObject.end()) {
            auto count = it->second.size();
            if (tablet.IsAliveOnLocal(Local)) {
                --count;
            }
            return count;
        } else {
            return 0;
        }
    }

    bool IsUnknown() const {
        return VolatileState == EVolatileState::Unknown;
    }

    bool IsAlive() const {
        return VolatileState == EVolatileState::Connected && (bool)Local;
    }

    bool IsRegistered() const {
        return VolatileState == EVolatileState::Connecting || VolatileState == EVolatileState::Connected;
    }

    bool MatchesFilter(const TNodeFilter& filter, TTabletDebugState* debugState = nullptr) const;
    bool IsAllowedToRunTablet(TTabletDebugState* debugState = nullptr) const;
    bool IsAllowedToRunTablet(const TTabletInfo& tablet, TTabletDebugState* debugState = nullptr) const;
    bool IsAbleToRunTablet(const TTabletInfo& tablet, TTabletDebugState* debugState = nullptr) const;
    i32 GetPriorityForTablet(const TTabletInfo& tablet) const;
    ui64 GetMaxTabletsScheduled() const;
    ui64 GetMaxCountForTabletType(TTabletTypes::EType tabletType) const;

    bool IsAbleToScheduleTablet() const {
        return GetTabletsScheduled() < GetMaxTabletsScheduled();
    }

    bool IsDisconnecting() const {
        return VolatileState == EVolatileState::Disconnecting;
    }

    bool IsDisconnected() const {
        return VolatileState == EVolatileState::Disconnected;
    }

    bool IsOverloaded() const;

    TString DumpTablets() const {
        TStringStream stream;
        for (const auto& t : Tablets) {
            stream << TTabletInfo::EVolatileStateName(t.first) << ":\n";
            for (const auto& j : t.second) {
                stream << j->ToString() << " " << TTabletInfo::EVolatileStateName(j->GetVolatileState()) << '\n';
            }
        }
        return stream.Str();
    }

    bool BecomeConnecting() {
        if (VolatileState != EVolatileState::Connecting) {
            ChangeVolatileState(EVolatileState::Connecting);
            return true;
        } else {
            return false;
        }
    }

    bool BecomeConnected();

    bool BecomeDisconnected() {
        if (VolatileState != EVolatileState::Disconnected) {
            TVector<TTabletInfo*> TabletsToRestart;
            for (const auto& t : Tablets) {
                for (const auto& j : t.second) {
                    TabletsToRestart.push_back(j);
                }
            }
            for (const auto& t : TabletsToRestart) {
                t->BecomeStopped();
            }
            Y_ABORT_UNLESS(GetTabletsTotal() == 0, "%s", DumpTablets().data());
            Local = TActorId();
            ChangeVolatileState(EVolatileState::Disconnected);
            for (TTabletInfo* tablet : TabletsToRestart) {
                if (tablet->IsReadyToBoot()) {
                    tablet->InitiateBoot();
                }
            }
            return true;
        }
        return false;
    }

    bool BecomeDisconnecting() {
        if (VolatileState == EVolatileState::Connected) {
            ChangeVolatileState(EVolatileState::Disconnecting);
            return true;
        } else {
            return false;
        }
    }

    bool CanBeDeleted() const;
    void RegisterInDomains();
    void DeregisterInDomains();
    void Ping();
    void SendReconnect(const TActorId& local);
    void SetDown(bool down);
    void SetFreeze(bool freeze);
    void UpdateResourceMaximum(const NKikimrTabletBase::TMetrics& metrics);

    TResourceRawValues GetResourceCurrentValues() const;

    const TResourceRawValues& GetResourceMaximumValues() const {
        return ResourceMaximumValues;
    }

    double GetNodeUsageForTablet(const TTabletInfo& tablet) const;
    double GetNodeUsage(EResourceToBalance resource = EResourceToBalance::ComputeResources) const;
    double GetNodeUsage(const TResourceNormalizedValues& normValues,
                        EResourceToBalance resource = EResourceToBalance::ComputeResources) const;

    ui64 GetTabletsRunningByType(TTabletTypes::EType tabletType) const;

    TResourceRawValues GetResourceInitialMaximumValues();
    TResourceRawValues GetStDevResourceValues();

    TDuration GetUptime() const {
        return TInstant::Now() - StartTime;
    }

    TString DebugServicedDomains() const {
        return TStringBuilder() << ServicedDomains;
    }

    TSubDomainKey GetServicedDomain() const {
        return ServicedDomains.empty() ? TSubDomainKey() : ServicedDomains.front();
    }

    void UpdateResourceTotalUsage(const NKikimrHive::TEvTabletMetrics& metrics);
    void ActualizeNodeStatistics(TInstant now);
    ui64 GetRestartsPerPeriod(TInstant barrier) const;

    TDataCenterId GetDataCenter() const {
        return Location.GetDataCenterId();
    }
};

} // NHive
} // NKikimr
