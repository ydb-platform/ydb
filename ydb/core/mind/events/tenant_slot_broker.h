#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/location.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <tuple>
#include <utility>
#include <ydb/core/protos/tenant_slot_broker.pb.h>

namespace NKikimr {
namespace NTenantSlotBroker {

static const TString ANY_DATA_CENTER = "";
constexpr char ANY_SLOT_TYPE[] = "";

constexpr char PIN_DATA_CENTER[] = "pinned";
constexpr char PIN_SLOT_TYPE[] = "pinned";

struct TEvTenantSlotBroker {
    enum EEv {
        // requests
        EvGetTenantState = EventSpaceBegin(TKikimrEvents::ES_TENANT_SLOT_BROKER),
        EvAlterTenant,
        EvRegisterPool,
        EvListTenants,

        // responses
        EvTenantState,
        EvTenantsList,

        // slot stats
        EvGetSlotStats,
        EvSlotStats,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_SLOT_BROKER),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TENANT_SLOT_BROKER)");

    struct TEvGetTenantState : public TEventPB<TEvGetTenantState, NKikimrTenantSlotBroker::TGetTenantState, EvGetTenantState> {};

    struct TEvAlterTenant : public TEventPB<TEvAlterTenant, NKikimrTenantSlotBroker::TAlterTenant, EvAlterTenant> {};

    struct TEvRegisterPool : public TEventPB<TEvRegisterPool, NKikimrTenantSlotBroker::TRegisterPool, EvRegisterPool> {};

    struct TEvListTenants : public TEventPB<TEvListTenants, NKikimrTenantSlotBroker::TListTenants, EvListTenants> {};

    struct TEvTenantState : public TEventPB<TEvTenantState, NKikimrTenantSlotBroker::TTenantState, EvTenantState> {};

    struct TEvTenantsList : public TEventPB<TEvTenantsList, NKikimrTenantSlotBroker::TTenantsList, EvTenantsList> {};

    struct TEvGetSlotStats : public TEventPB<TEvGetSlotStats, NKikimrTenantSlotBroker::TGetSlotStats, EvGetSlotStats> {};

    struct TEvSlotStats : public TEventPB<TEvSlotStats, NKikimrTenantSlotBroker::TSlotStats, EvSlotStats> {};

};

IActor *CreateTenantSlotBroker(const TActorId &tablet, TTabletStorageInfo *info);

} // NTenantSlotBroker
} // namespace NKikimr

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

