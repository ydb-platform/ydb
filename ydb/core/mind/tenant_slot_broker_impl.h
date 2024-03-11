#pragma once

#include "tenant_slot_broker.h"
#include "tenant_slot_broker__scheme.h"

#include <ydb/core/base/location.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/protos/tenant_slot_broker.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NTenantSlotBroker {

struct TSlotId {
    ui32 NodeId = 0;
    TString SlotId;

    TSlotId(ui32 nodeId = 0,
            const TString &slotId = "")
        : NodeId(nodeId)
        , SlotId(slotId)
    {
    }

    TSlotId(const NKikimrTenantSlotBroker::TSlotId &rec)
    {
        Load(rec);
    }

    void Load(const NKikimrTenantSlotBroker::TSlotId &rec)
    {
        NodeId = rec.GetNodeId();
        SlotId = rec.GetSlotId();
    }

    bool operator==(const TSlotId &other) const
    {
        return (NodeId == other.NodeId
                && SlotId == other.SlotId);
    }

    bool operator!=(const TSlotId &other) const
    {
        return !(*this == other);
    }

    bool operator<(const NKikimr::NTenantSlotBroker::TSlotId &rhs) const
    {
        if (NodeId == rhs.NodeId)
            return SlotId < rhs.SlotId;
        return NodeId < rhs.NodeId;
    }

    TString ToString() const
    {
        return TStringBuilder() << "[" << NodeId << ", " << SlotId << "]";
    }
};

struct TSlotDescription {
    TString DataCenter;
    bool ForceLocation = true;
    ui32 CollocationGroup = 0;
    bool ForceCollocation = false;
    TString SlotType;

    TSlotDescription() = default;

    TSlotDescription(const TString &type,
                     const TString &dc,
                     bool forceLocation = true,
                     ui32 group = 0,
                     bool forceCollocation = false)
        : DataCenter(dc)
        , ForceLocation(forceLocation)
        , CollocationGroup(group)
        , ForceCollocation(forceCollocation)
        , SlotType(type)
    {
    }

    TSlotDescription(const NKikimrTenantSlotBroker::TSlotAllocation &slot)
    {
        SlotType = slot.GetType();
        DataCenter = slot.HasDataCenter() ? slot.GetDataCenter() : DataCenterToString(slot.GetDataCenterNum());
        ForceLocation = slot.GetForceLocation();
        CollocationGroup = slot.GetCollocationGroup();
        ForceCollocation = slot.GetForceCollocation();
    }

    TSlotDescription(const TSlotDescription &) = default;
    TSlotDescription(TSlotDescription &&) = default;

    TSlotDescription &operator=(const TSlotDescription &) = default;
    TSlotDescription &operator=(TSlotDescription &&) = default;

    bool operator==(const NKikimr::NTenantSlotBroker::TSlotDescription &rhs) const
    {
        return (DataCenter == rhs.DataCenter
                && CollocationGroup == rhs.CollocationGroup
                && ForceLocation == rhs.ForceLocation
                && ForceCollocation == rhs.ForceCollocation
                && SlotType == rhs.SlotType);
    }

    bool operator!=(const NKikimr::NTenantSlotBroker::TSlotDescription &rhs) const
    {
        return !(*this == rhs);
    }

    void Serialize(NKikimrTenantSlotBroker::TSlotAllocation &slot) const
    {
        slot.SetType(SlotType);
        slot.SetDataCenterNum(DataCenterFromString(DataCenter));
        slot.SetDataCenter(DataCenter);
        slot.SetForceLocation(ForceLocation);
        slot.SetCollocationGroup(CollocationGroup);
        slot.SetForceCollocation(ForceCollocation);
    }

    std::pair<TString, TString> CountersKey() const
    {
        return std::make_pair(SlotType, DataCenter);
    }

    TString ToString() const
    {
        TStringBuilder str;
        str << "[" << SlotType << ", " << DataCenter;
        if (!ForceLocation)
            str << "*";
        if (CollocationGroup)
            str << "(" << CollocationGroup
                << (ForceCollocation ? "" : "*") << ")";
        str << "]";
        return str;
    }
};

} // NTenantSlotBroker
} // NKikimr

template<>
struct THash<NKikimr::NTenantSlotBroker::TSlotId> {
    inline size_t operator()(const NKikimr::NTenantSlotBroker::TSlotId &id) const {
        auto t = std::make_pair(id.NodeId, id.SlotId);
        return THash<decltype(t)>()(t);
    }
};

template<>
struct THash<NKikimr::NTenantSlotBroker::TSlotDescription> {
    inline size_t operator()(const NKikimr::NTenantSlotBroker::TSlotDescription &descr) const {
        auto t = std::make_tuple(descr.DataCenter, descr.ForceLocation, descr.CollocationGroup,
                                 descr.ForceCollocation, descr.SlotType);
        return THash<decltype(t)>()(t);
    }
};

namespace NKikimr {
namespace NTenantSlotBroker {

using NConsole::TEvConsole;
using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;
using ::NMonitoring::TDynamicCounterPtr;
using ::NMonitoring::TDynamicCounters;

class TTenantSlotBroker : public TActor<TTenantSlotBroker>, public TTabletExecutedFlat {
private:
    using TActorBase = TActor<TTenantSlotBroker>;

    static const ui32 ConfigKey_Config;

    class TCounters : public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TCounters>;

    private:
        enum ESlotKind {
            KIND_FREE = 0,
            KIND_ASSIGNED,
            KIND_PENDING,
            KIND_MISSING,
            KIND_REQUIRED,
            KIND_CONNECTED,
            KIND_DISCONNECTED,
            KIND_MISPLACED,
            KIND_SPLIT,
            KIND_BANNED,
            KIND_PINNED,
            KIND_COUNT
        };

        using TCountersMap = THashMap<std::pair<TString, TString>, TDynamicCounters::TCounterPtr>;

        const std::array<TString, KIND_COUNT> SlotSensorNames = {{
            { "FreeSlots" },
            { "AssignedSlots" },
            { "PendingSlots" },
            { "MissingSlots" },
            { "RequiredSlots" },
            { "ConnectedSlots" },
            { "DisconnectedSlots" },
            { "MisplacedSlots" },
            { "SplitSlots" },
            { "BannedSlots" },
            { "PinnedSlots" } }};

        std::array<TCountersMap, KIND_COUNT> SlotCounters;

        TDynamicCounters::TCounterPtr Slots(const std::pair<TString, TString> &key, ESlotKind kind)
        {
            auto &counters = SlotCounters[kind];
            auto it = counters.find(key);
            if (it != counters.end())
                return it->second;

            auto type = Counters->GetSubgroup("SlotType", key.first);
            auto dc = type->GetSubgroup("SlotDataCenter", key.second);
            auto counter = dc->GetCounter(SlotSensorNames[kind]);

            counters[key] = counter;
            return counter;
        }

    public:
        TDynamicCounterPtr Counters;
        TDynamicCounters::TCounterPtr KnownTenants;
        TDynamicCounters::TCounterPtr UnhappyTenants;
        TDynamicCounters::TCounterPtr MisplacedTenants;
        TDynamicCounters::TCounterPtr SplitTenants;
        TDynamicCounters::TCounterPtr ConnectedPools;

        TCounters(TDynamicCounterPtr counters)
            : Counters(counters)
        {
            KnownTenants = Counters->GetCounter("KnownTenants");
            UnhappyTenants = Counters->GetCounter("UnhappyTenants");
            MisplacedTenants = Counters->GetCounter("MisplacedTenants");
            SplitTenants = Counters->GetCounter("SplitTenants");
            ConnectedPools = Counters->GetCounter("ConnectedPools");
        }

        void Clear()
        {
            *KnownTenants = 0;
            *UnhappyTenants = 0;
            *MisplacedTenants = 0;
            *SplitTenants = 0;
            *ConnectedPools = 0;
            for (auto &slots : SlotCounters)
                for (auto &pr : slots)
                    *pr.second = 0;
        }

        TDynamicCounterPtr AllCounters() { return Counters; }

        TDynamicCounters::TCounterPtr FreeSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_FREE);
        }

        TDynamicCounters::TCounterPtr FreeSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_FREE);
        }

        TDynamicCounters::TCounterPtr AssignedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_ASSIGNED);
        }

        TDynamicCounters::TCounterPtr AssignedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_ASSIGNED);
        }

        TDynamicCounters::TCounterPtr PendingSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_PENDING);
        }

        TDynamicCounters::TCounterPtr PendingSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_PENDING);
        }

        TDynamicCounters::TCounterPtr MissingSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_MISSING);
        }

        TDynamicCounters::TCounterPtr MissingSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_MISSING);
        }

        TDynamicCounters::TCounterPtr RequiredSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_REQUIRED);
        }

        TDynamicCounters::TCounterPtr RequiredSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_REQUIRED);
        }

        TDynamicCounters::TCounterPtr ConnectedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_CONNECTED);
        }

        TDynamicCounters::TCounterPtr ConnectedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_CONNECTED);
        }

        TDynamicCounters::TCounterPtr DisconnectedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_DISCONNECTED);
        }

        TDynamicCounters::TCounterPtr DisconnectedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_DISCONNECTED);
        }

        TDynamicCounters::TCounterPtr MisplacedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_MISPLACED);
        }

        TDynamicCounters::TCounterPtr MisplacedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_MISPLACED);
        }

        TDynamicCounters::TCounterPtr SplitSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_SPLIT);
        }

        TDynamicCounters::TCounterPtr SplitSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_SPLIT);
        }

        TDynamicCounters::TCounterPtr BannedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_BANNED);
        }

        TDynamicCounters::TCounterPtr BannedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_BANNED);
        }

        TDynamicCounters::TCounterPtr PinnedSlots(const std::pair<TString, TString> &key)
        {
            return Slots(key, KIND_PINNED);
        }

        TDynamicCounters::TCounterPtr PinnedSlots(const TString &type, const TString &dc)
        {
            return Slots(std::make_pair(type, dc), KIND_PINNED);
        }

        void FillSlotStats(NKikimrTenantSlotBroker::TSlotStats &stats)
        {
            // <Type, DataCenter> -> <Connected, Free>
            THashMap<std::pair<TString, TString>, std::pair<ui64, ui64>> counts;
            auto count = [&counts, this](const TString& name, const TString& value) {
                    if (name != "SlotType")
                        return;
                    auto group = Counters->GetSubgroup(name, value);
                    group->EnumerateSubgroups([&counts, this, type = value, group](const TString& name, const TString& value) {
                                if (name != "SlotDataCenter")
                                    return;
                                auto dcGroup = group->GetSubgroup(name, value);
                                if (dcGroup) {
                                    auto connected = dcGroup->GetCounter(SlotSensorNames[KIND_CONNECTED])->Val();
                                    auto free = dcGroup->GetCounter(SlotSensorNames[KIND_FREE])->Val();
                                    if (connected)
                                        counts[std::make_pair(type, value)] = std::make_pair(connected, free);
                                }
                            });
                };

            // Fill counts map.
            Counters->EnumerateSubgroups(count);

            for (auto &pr : counts) {
                auto &rec = *stats.AddSlotCounters();
                rec.SetType(pr.first.first);
                rec.SetDataCenter(pr.first.second);
                rec.SetConnected(pr.second.first);
                rec.SetFree(pr.second.second);
            }
        }
    };

    struct TSlotsAllocation;

    struct TTenantStatistics : public TThrRefBase {
        using TPtr = TIntrusivePtr<TTenantStatistics>;

        TTenantStatistics(TCounters::TPtr counters)
            : TotalRequired(0)
            , TotalMissing(0)
            , TotalPending(0)
            , TotalMisplaced(0)
            , TotalSplit(0)
            , TotalPinned(0)
            , Counters(counters)
        {
        }

        void Clear()
        {
            Pending.clear();
            Missing.clear();
            Misplaced.clear();
            Split.clear();
        }

        ui64 TotalRequired;
        ui64 TotalMissing;
        ui64 TotalPending;
        ui64 TotalMisplaced;
        ui64 TotalSplit;
        ui64 TotalPinned;
        TCounters::TPtr Counters;
        THashSet<TIntrusivePtr<TSlotsAllocation>> Pending;
        THashSet<TIntrusivePtr<TSlotsAllocation>> Missing;
        THashSet<TIntrusivePtr<TSlotsAllocation>> Misplaced;
        THashSet<TIntrusivePtr<TSlotsAllocation>> Split;
    };

    class TTenant;

    struct TSlot : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSlot>;

        TSlot(const TSlotId &id, const TString &type, const TString &dc)
            : Id(id)
            , Type(type)
            , DataCenter(dc)
            , IsConnected(false)
            , IsBanned(false)
            , IsPinned(false)
            , LastRequestId(0)
        {
        }

        TString IdString(bool full = false)
        {
            if (!full)
                return Id.ToString();
            TStringBuilder str;
            if (!AssignedTenant)
                str << "free ";
            str << "slot [" << Id.NodeId << ", " << Id.SlotId << ", " << Type << ", " << DataCenter << "]";
            if (IsBanned)
                str << " banned";
            if (AssignedTenant)
                str << (IsPinned ? " pinned to " : " used by ")
                    << AssignedTenant->Name << " as " << Label;
            return str;
        }

        bool IsFree() const { return !AssignedTenant && !LastRequestId && !IsBanned && !IsPinned; }
        bool IsPending() const { return AssignedTenant && LastRequestId; }

        TSlotId Id;
        TString Type;
        TString DataCenter;
        TIntrusivePtr<TTenant> AssignedTenant;
        TSlotDescription UsedAs;
        bool IsConnected;
        bool IsBanned;
        bool IsPinned;
        // Non-zero value means resource is pending.
        ui64 LastRequestId;
        // Each assigned slot has a label used in monitoring.
        // Labels should be unique for slots of a single tenant.
        TString Label;
    };

    struct TPinnedSlotInfo {
        TString TenantName;
        TString Label;
        TInstant Deadline;
    };

    struct TEnqueuedUnpin {
        TSlotId SlotId;
        bool Scheduled;
    };

    using TSlotSet = THashSet<TSlot::TPtr, TPtrHash>;

    class TCollocationGroup : public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TCollocationGroup>;

        TCollocationGroup(ui32 id)
            : Id(id)
        {
        }

        const TString &GetPreferredDataCenter() const { return PreferredDataCenter; }
        void SetPreferredDataCenter(const TString &dc);

        const ui32 Id;
        THashSet<TIntrusivePtr<TSlotsAllocation>> Allocations;

    private:
        TString PreferredDataCenter;
    };

    struct TSlotsAllocation : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSlotsAllocation>;

        TSlotDescription Description;
        TCollocationGroup::TPtr Group;
        TSlotSet AssignedSlots;
        ui64 RequiredCount;
        ui64 MissingCount;
        ui64 PendingCount;
        ui64 MisplacedCount;
        ui64 SplitCount;
        ui64 PinnedCount;
        TTenantStatistics::TPtr Stats;

        TSlotsAllocation(const TSlotDescription &descr,
                         TTenantStatistics::TPtr stats)
            : Description(descr)
            , RequiredCount(0)
            , MissingCount(0)
            , PendingCount(0)
            , MisplacedCount(0)
            , SplitCount(0)
            , PinnedCount(0)
            , Stats(stats)
        {

        }

        void AddAssignedSlot(TSlot::TPtr slot)
        {
            Y_ABORT_UNLESS(!AssignedSlots.contains(slot));
            AssignedSlots.insert(slot);
        }

        void RemoveAssignedSlot(TSlot::TPtr slot)
        {
            Y_ABORT_UNLESS(AssignedSlots.contains(slot));
            AssignedSlots.erase(slot);
        }

        void SetPending(ui64 count)
        {
            ui64 diff = count - PendingCount;
            PendingCount = count;
            Stats->TotalPending += diff;
            Stats->Counters->PendingSlots(Description.CountersKey())->Add(diff);

            if (count)
                Stats->Pending.insert(this);
            else
                Stats->Pending.erase(this);
        }

        void IncPending()
        {
            SetPending(PendingCount + 1);
        }

        void DecPending()
        {
            Y_ABORT_UNLESS(PendingCount, "Dec zero pending for %s", Description.ToString().data());
            SetPending(PendingCount - 1);
        }

        void SetMissing(ui64 count)
        {
            ui64 diff = count - MissingCount;
            MissingCount = count;
            Stats->TotalMissing += diff;
            Stats->Counters->MissingSlots(Description.CountersKey())->Add(diff);

            if (count) {
                // Insertion of existing element to THashSet
                // can invalidate iterators and it might break
                // free slots allocation which iterates through
                // the Missing set.
                if (!Stats->Missing.contains(this))
                    Stats->Missing.insert(this);
            } else
                Stats->Missing.erase(this);
        }

        void IncMissing(ui64 d = 1)
        {
            SetMissing(MissingCount + d);
        }

        void DecMissing()
        {
            Y_ABORT_UNLESS(MissingCount, "Dec zero missing for %s", Description.ToString().data());
            SetMissing(MissingCount - 1);
        }

        void SetMisplaced(ui64 count)
        {
            ui64 diff = count - MisplacedCount;
            MisplacedCount = count;
            Stats->TotalMisplaced += diff;
            Stats->Counters->MisplacedSlots(Description.CountersKey())->Add(diff);

            if (count)
                Stats->Misplaced.insert(this);
            else
                Stats->Misplaced.erase(this);
        }

        void IncMisplaced()
        {
            SetMisplaced(MisplacedCount + 1);
        }

        void DecMisplaced()
        {
            Y_ABORT_UNLESS(MisplacedCount, "Dec zero misplaced for %s", Description.ToString().data());
            SetMisplaced(MisplacedCount - 1);
        }

        void SetSplit(ui64 count)
        {
            ui64 diff = count - SplitCount;
            SplitCount = count;
            Stats->TotalSplit += diff;
            Stats->Counters->SplitSlots(Description.CountersKey())->Add(diff);

            if (count)
                Stats->Split.insert(this);
            else
                Stats->Split.erase(this);
        }

        void IncSplit()
        {
            SetSplit(SplitCount + 1);
        }

        void DecSplit()
        {
            Y_ABORT_UNLESS(SplitCount, "Dec zero split for %s", Description.ToString().data());
            SetSplit(SplitCount - 1);
        }

        void SetPinned(ui64 count)
        {
            ui64 diff = count - PinnedCount;
            PinnedCount = count;
            Stats->TotalPinned += diff;
        }

        void IncPinned()
        {
            SetPinned(PinnedCount + 1);
        }

        void DecPinned()
        {
            Y_ABORT_UNLESS(PinnedCount, "Dec zero pinned for %s", Description.ToString().data());
            SetPinned(PinnedCount - 1);
        }

        void SetRequired(ui64 count)
        {
            ui64 diff = count - RequiredCount;
            RequiredCount = count;
            Stats->TotalRequired += diff;
            Stats->Counters->RequiredSlots(Description.CountersKey())->Add(diff);
        }

        bool IsSlotOk(const TString &type,
                      const TString &dc,
                      bool strictMatch = true) const;

        void RecomputeSplitCount();
    };

    struct TLayout : public TThrRefBase {
        using TPtr = TIntrusivePtr<TLayout>;

        TString ToString() const
        {
            return TStringBuilder() << "m(" << MissingCount
                                    << ") mp(" << MisplacedCount
                                    << ") s(" << SplitCount
                                    << ") d(" << DetachCount << ")";
        }

        THashMap<TSlotsAllocation::TPtr, TSlotSet> AssignedSlots;
        ui64 MissingCount = 0;
        ui64 MisplacedCount = 0;
        ui64 SplitCount = 0;
        ui64 DetachCount = 0;
    };

    class TTenant : public TThrRefBase{
    public:
        using TPtr = TIntrusivePtr<TTenant>;

        TTenant(const TString &name, TCounters::TPtr counters)
            : Name(name)
            , Counters(counters)
            , Stats(new TTenantStatistics(counters))
        {
        }

        ~TTenant()
        {
            for (auto &pr : Groups)
                pr.second->Allocations.clear();
            Stats->Clear();
        }

        const THashMap<TSlotDescription, TSlotsAllocation::TPtr> &GetAllocations() const
        {
            return Allocations;
        }

        TSlotsAllocation::TPtr GetAllocation(const TSlotDescription &key) const
        {
            auto it = Allocations.find(key);
            if (it == Allocations.end())
                return nullptr;
            return it->second;
        }

        ui64 GetRequired(const TSlotDescription &key) const
        {
            auto it = Allocations.find(key);
            if (it == Allocations.end())
                return 0;
            return it->second->RequiredCount;
        }

        const THashSet<TSlotsAllocation::TPtr> &GetMissing() const
        {
            return Stats->Missing;
        }

        const THashSet<TSlotsAllocation::TPtr> &GetMisplaced() const
        {
            return Stats->Misplaced;
        }

        const THashSet<TSlotsAllocation::TPtr> &GetSplit() const
        {
            return Stats->Split;
        }

        void AddSlotsAllocation(const TSlotDescription &descr,
                                ui64 count);
        void ClearEmptyAllocations();
        TCollocationGroup::TPtr GetOrCreateCollocationGroup(ui32 group);
        void DetermineDataCenterForCollocationGroups();

        TString SlotLabelByNo(ui64 no) const;
        void AddSlotLabels(ui64 count,
                           TTransactionContext &txc);
        void RemoveSlotLabels(ui64 count,
                              TTransactionContext &txc);
        void MarkSlotLabelAsUsed(const TString &label);
        void MarkSlotLabelAsUnused(const TString &label);
        TString GetFirstUnusedSlotLabel() const;
        void AddUnusedSlotLabel(const TString &label);

        // Get label for new pinned slot.
        TString MakePinnedSlotLabel();
        void AddPinnedSlotLabel(const TString &label);
        void RemovePinnedSlotLabel(const TString &label);

        void AddPinnedSlot(TSlot::TPtr slot);
        void RemovePinnedSlot(TSlot::TPtr slot);

        ui64 GetTotalMissing() const { return Stats->TotalMissing; }
        ui64 GetTotalMisplaced() const { return Stats->TotalMisplaced; }
        ui64 GetTotalPending() const { return Stats->TotalPending; }
        ui64 GetTotalRequired() const { return Stats->TotalRequired; }
        ui64 GetTotalSplit() const { return Stats->TotalSplit; }

        bool CanBeRemoved() const;

        void DbUpdateAllocation(const TSlotDescription &key,
                                TTransactionContext &txc);

        const TString Name;

    private:
        TSlotsAllocation::TPtr GetOrCreateAllocation(const TSlotDescription &descr);

        static const TSlotDescription PinnedSlotDescription;

        // <Type,DataCenter> -> count
        THashMap<TSlotDescription, TSlotsAllocation::TPtr> Allocations;
        THashMap<ui32, TCollocationGroup::TPtr> Groups;
        TCounters::TPtr Counters;
        TTenantStatistics::TPtr Stats;
        TSet<TString> UsedSlotLabels;
        TSet<TString> UnusedSlotLabels;
        THashSet<TString> PinnedSlotLabels;
    };

    struct TTenantLessHappy {
        bool operator()(const TTenant::TPtr &lhs, const TTenant::TPtr &rhs) const
        {
            auto ratio1 = lhs->GetTotalRequired() * rhs->GetTotalMissing();
            auto ratio2 = lhs->GetTotalMissing() * rhs->GetTotalRequired();
            if (ratio1 != ratio2)
                return ratio1 < ratio2;
            return lhs.Get() < rhs.Get();
        }
    };

    struct TTenantMoreMisplaced {
        bool operator()(const TTenant::TPtr &lhs, const TTenant::TPtr &rhs) const
        {
            auto ratio1 = lhs->GetTotalRequired() * rhs->GetTotalMisplaced();
            auto ratio2 = lhs->GetTotalMisplaced() * rhs->GetTotalRequired();
            if (ratio1 != ratio2)
                return ratio1 < ratio2;
            return lhs.Get() < rhs.Get();
        }
    };

    struct TTenantMoreSplit {
        bool operator()(const TTenant::TPtr &lhs, const TTenant::TPtr &rhs) const
        {
            auto ratio1 = lhs->GetTotalRequired() * rhs->GetTotalSplit();
            auto ratio2 = lhs->GetTotalSplit() * rhs->GetTotalRequired();
            if (ratio1 != ratio2)
                return ratio1 < ratio2;
            return lhs.Get() < rhs.Get();
        }
    };

    struct TFreeSlotsIndex {
        void Add(TSlot::TPtr slot);
        void Remove(TSlot::TPtr slot);

        TSlot::TPtr Find();
        TSlot::TPtr FindByType(const TString &type);
        TSlot::TPtr FindByDC(const TString &dataCenter);
        TSlot::TPtr Find(const TString &type, const TString &dataCenter);

        bool Empty() const
        {
            return FreeSlotsByType.empty();
        }

        // Type -> {DataCenter -> {Slots}}
        THashMap<TString, THashMap<TString, TSlotSet>> FreeSlotsByType;
        // DataCenter -> {Type -> {Slots}}
        THashMap<TString, THashMap<TString, TSlotSet>> FreeSlotsByDataCenter;
    };

public:
    struct TEvPrivate {
        enum EEv {
            EvCheckSlotStatus = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvCheckAllSlotsStatus,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvCheckSlotStatus : public TEventLocal<TEvCheckSlotStatus, EvCheckSlotStatus> {
            TEvCheckSlotStatus(TSlot::TPtr slot, ui64 requestId)
                : Slot(slot)
                , RequestId(requestId)
            {
            }

            TSlot::TPtr Slot;
            ui64 RequestId;
        };

        struct TEvCheckAllSlotsStatus : public TEventLocal<TEvCheckAllSlotsStatus, EvCheckAllSlotsStatus> {
            TEvCheckAllSlotsStatus(ui64 requestId)
                : RequestId(requestId)
            {
            }

            ui64 RequestId;
        };

    };

private:
    class TTxAlterTenant;
    class TTxAssignFreeSlots;
    class TTxBanSlot;
    class TTxCheckSlotStatus;
    class TTxInitScheme;
    class TTxLoadState;
    class TTxPinSlot;
    class TTxUnbanSlot;
    class TTxUnpinSlotImpl;
    class TTxUnpinSlot;
    class TTxUpdateConfig;
    class TTxUpdateNodeLocation;
    class TTxUpdatePoolStatus;
    class TTxUpdateSlotStatus;

    ITransaction *CreateTxAlterTenant(TEvTenantSlotBroker::TEvAlterTenant::TPtr &ev);
    ITransaction *CreateTxAssignFreeSlots();
    ITransaction *CreateTxCheckSlotStatus(ui64 requestId,
                                          TSlot::TPtr slot = nullptr);
    ITransaction *CreateTxInitScheme();
    ITransaction *CreateTxLoadState();
    ITransaction *CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);
    ITransaction *CreateTxUpdateNodeLocation(TEvInterconnect::TEvNodeInfo::TPtr &ev);
    ITransaction *CreateTxUpdatePoolStatus(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev);
    ITransaction *CreateTxUpdateSlotStatus(TEvTenantPool::TEvConfigureSlotResult::TPtr &ev);


    ui64 Generation() const;
    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev,
                      const TActorContext &ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
                             const TActorContext &ctx) override;

    void Cleanup(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

    void LoadConfigFromProto(const NKikimrTenantSlotBroker::TConfig &config);
    void SwitchToWork(const TActorContext &ctx);

    void ClearState();

    TTenant::TPtr GetTenant(const TString &name);
    TTenant::TPtr AddTenant(const TString &name);
    TTenant::TPtr GetOrCreateTenant(const TString &name);
    void MaybeRemoveTenant(TTenant::TPtr tenant);

    void AddUnhappyTenant(TTenant::TPtr tenant);
    void RemoveUnhappyTenant(TTenant::TPtr tenant);
    bool HasUnhappyTenant() const;

    TSlot::TPtr GetSlot(const TSlotId &id);
    TSlot::TPtr GetSlot(ui32 nodeId, const TString &id);
    void AddSlot(TSlot::TPtr slot);
    void AddSlot(TSlot::TPtr slot,
                 TTransactionContext &txc,
                 const TActorContext &ctx);

    void SlotConnected(TSlot::TPtr slot);
    void SlotDisconnected(TSlot::TPtr slot);
    bool UpdateSlotDataCenter(TSlot::TPtr slot,
                              const TString &dataCenter,
                              TTransactionContext &txc,
                              const TActorContext &ctx);
    bool UpdateSlotType(TSlot::TPtr slot,
                        const TString &type,
                        TTransactionContext &txc,
                        const TActorContext &ctx);
    void RemoveSlot(TSlot::TPtr slot,
                    TTransactionContext &txc,
                    const TActorContext &ctx);
    void AddSlotToCounters(TSlot::TPtr slot);
    void RemoveSlotFromCounters(TSlot::TPtr slot);
    void DetachSlotNoConfigureNoDb(TSlot::TPtr slot,
                                   bool updateUnhappy = true);
    void DetachSlotNoConfigure(TSlot::TPtr slot,
                               TTransactionContext &txc,
                               bool updateUnhappy = true);
    void DetachSlot(TSlot::TPtr slot,
                    TTransactionContext &txc,
                    const TActorContext &ctx,
                    bool updateUnhappy = true);
    void AttachSlotNoConfigureNoDb(TSlot::TPtr slot,
                                   TTenant::TPtr tenant,
                                   const TSlotDescription &usedAs,
                                   const TString &label);
    void AttachSlotNoConfigure(TSlot::TPtr slot,
                               TTenant::TPtr tenant,
                               const TSlotDescription &usedAs,
                               const TString &label,
                               TTransactionContext &txc);
    void AttachSlot(TSlot::TPtr slot,
                    TTenant::TPtr tenant,
                    const TSlotDescription &usedAs,
                    TTransactionContext &txc,
                    const TActorContext &ctx);
    bool MoveMisplacedSlots(TTenant::TPtr tenant,
                            TSlotsAllocation::TPtr allocation,
                            bool singleSlot,
                            TTransactionContext &txc,
                            const TActorContext &ctx);
    bool AssignFreeSlots(TTenant::TPtr tenant,
                         bool singleSlot,
                         TTransactionContext &txc,
                         const TActorContext &ctx);
    TSlot::TPtr ExtractSlot(TFreeSlotsIndex &primary,
                            TFreeSlotsIndex &secondary,
                            const TString &type,
                            const TString &dc) const;
    TLayout::TPtr ComputeLayoutForGroup(TCollocationGroup::TPtr group,
                                        const TString &dc);
    double ComputeLayoutPenalty(ui64 required,
                                ui64 missing,
                                ui64 misplaced,
                                ui64 split,
                                ui64 detached) const;
    void ApplyLayout(TTenant::TPtr tenant,
                     TLayout::TPtr layout,
                     TTransactionContext &txc,
                     const TActorContext &ctx);
    bool AssignFreeSlotsForGroup(TTenant::TPtr tenant,
                                 TCollocationGroup::TPtr group,
                                 TTransactionContext &txc,
                                 const TActorContext &ctx);
    void OnClientDisconnected(TActorId clientId,
                              const TActorContext &ctx);
    void DisconnectNodeSlots(ui32 nodeId,
                             const TActorContext &ctx);
    void SendConfigureSlot(TSlot::TPtr slot,
                           const TActorContext &ctx);
    void FillTenantState(const TString &name,
                         NKikimrTenantSlotBroker::TTenantState &state);
    void ScheduleTxAssignFreeSlots(const TActorContext &ctx);

    void ProcessTx(ITransaction *tx, const TActorContext &ctx);
    void TxCompleted(ITransaction *tx, const TActorContext &ctx);
    void ProcessNextTx(const TActorContext &ctx);

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvents::TEvUndelivered::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvPrivate::TEvCheckAllSlotsStatus::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvPrivate::TEvCheckSlotStatus::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerDestroyed::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantPool::TEvConfigureSlotResult::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantPool::TEvLostOwnership::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvAlterTenant::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvGetSlotStats::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvGetTenantState::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvListTenants::TPtr &ev,
                const TActorContext &ctx);
    void Handle(TEvTenantSlotBroker::TEvRegisterPool::TPtr &ev,
                const TActorContext &ctx);

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::TENANT_SLOT_BROKER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFuncTraced(TEvConsole::TEvReplaceConfigSubscriptionsResponse, Handle);
            HFuncTraced(TEvents::TEvUndelivered, Handle);
            HFuncTraced(TEvInterconnect::TEvNodeInfo, Handle);
            HFuncTraced(TEvPrivate::TEvCheckAllSlotsStatus, Handle);
            HFuncTraced(TEvPrivate::TEvCheckSlotStatus, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerConnected, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerDestroyed, Handle);
            HFuncTraced(TEvTabletPipe::TEvServerDisconnected, Handle);
            HFuncTraced(TEvTenantPool::TEvConfigureSlotResult, Handle);
            HFuncTraced(TEvTenantPool::TEvLostOwnership, Handle);
            HFuncTraced(TEvTenantPool::TEvTenantPoolStatus, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvAlterTenant, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvGetSlotStats, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvGetTenantState, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvListTenants, Handle);
            HFuncTraced(TEvTenantSlotBroker::TEvRegisterPool, Handle);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                Y_ABORT("TTenantSlotBroker::StateWork unexpected event type: %" PRIx32 " event: %s",
                       ev->GetTypeRewrite(), ev->ToString().data());
            }
        }
    }

public:
    TTenantSlotBroker(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , PendingAssignFreeSlots(false)
        , ConfigSubscriptionId(0)
    {
    }

    ~TTenantSlotBroker()
    {
        ClearState();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::TENANT_SLOT_BROKER_ACTOR;
    }

private:
    NKikimrTenantSlotBroker::TConfig Config;
    TDuration PendingTimeout;
    ui64 RequestId;
    TString DomainName;
    // NodeId -> DataCenter
    THashMap<ui32, TString> NodeIdToDataCenter;
    // TenantName -> Tenant
    THashMap<TString, TTenant::TPtr> Tenants;
    // <NodeId, SlotId> -> Slot
    THashMap<TSlotId, TSlot::TPtr> Slots;
    THashSet<TSlotId> BannedSlots;
    THashMap<TSlotId, TPinnedSlotInfo> PinnedSlots;
    TMap<TInstant, TEnqueuedUnpin> EnqueuedUnpins;
    // NodeId -> {Slots}
    THashMap<ui32, TSlotSet> SlotsByNodeId;
    TFreeSlotsIndex FreeSlots;
    // Tenants with missing, misplaced and split resources.
    TSet<TTenant::TPtr, TTenantLessHappy> UnhappyTenants;
    TSet<TTenant::TPtr, TTenantMoreMisplaced> MisplacedTenants;
    TSet<TTenant::TPtr, TTenantMoreSplit> SplitTenants;
    bool PendingAssignFreeSlots;
    THashSet<TActorId> KnownPoolPipes;
    TCounters::TPtr Counters;
    ITransaction *ActiveTx = nullptr;
    TDeque<THolder<ITransaction>> TxQueue;
    ui64 ConfigSubscriptionId;
};

} // NTenantSlotBroker
} // NKikimr
