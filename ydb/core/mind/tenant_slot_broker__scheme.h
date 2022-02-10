#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NTenantSlotBroker {

struct Schema : NIceDb::Schema {
    struct Config : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct RequiredSlots : Table<2> {
        struct TenantName : Column<1, NScheme::NTypeIds::Utf8> {};
        struct SlotType : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DataCenter : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Count : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TenantName, SlotType, DataCenter>;
        using TColumns = TableColumns<TenantName, SlotType, DataCenter, Count>;
    };

    struct Slots : Table<3> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SlotId : Column<2, NScheme::NTypeIds::Utf8> {};
        struct SlotType : Column<3, NScheme::NTypeIds::Utf8> {};
        struct DataCenter : Column<4, NScheme::NTypeIds::Uint64> {};
        struct AssignedTenant : Column<5, NScheme::NTypeIds::Utf8> {};
        struct UsedAsType : Column<6, NScheme::NTypeIds::Utf8> {};
        struct UsedAsDataCenter : Column<7, NScheme::NTypeIds::Uint64> {};
        struct Label : Column<8, NScheme::NTypeIds::Utf8> {};
        struct DataCenterName : Column<9, NScheme::NTypeIds::Utf8> {};
        struct UsedAsDataCenterName : Column<10, NScheme::NTypeIds::Utf8> {};
        struct UsedAsCollocationGroup : Column<11, NScheme::NTypeIds::Uint32> {};
        struct UsedAsForceLocation : Column<12, NScheme::NTypeIds::Bool> {};
        struct UsedAsForceCollocation : Column<13, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<NodeId, SlotId>;
        using TColumns = TableColumns<NodeId, SlotId, SlotType, DataCenter, AssignedTenant, UsedAsType, UsedAsDataCenter, Label,
            DataCenterName, UsedAsDataCenterName, UsedAsCollocationGroup, UsedAsForceLocation, UsedAsForceCollocation>;
    };

    struct SlotLabels : Table<4> {
        struct TenantName : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Label : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<TenantName, Label>;
        using TColumns = TableColumns<TenantName, Label>;
    };

    // TODO: move location into global table and reference it by ID here and in Slots UsedBy;
    struct SlotsAllocations : Table<5> {
        struct TenantName : Column<1, NScheme::NTypeIds::Utf8> {};
        struct SlotType : Column<2, NScheme::NTypeIds::Utf8> {};
        struct DataCenter : Column<3, NScheme::NTypeIds::Utf8> {};
        struct CollocationGroup : Column<4, NScheme::NTypeIds::Uint32> {};
        struct ForceLocation : Column<5, NScheme::NTypeIds::Bool> {};
        struct ForceCollocation : Column<6, NScheme::NTypeIds::Bool> {};
        struct Count : Column<7, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<TenantName, SlotType, DataCenter, CollocationGroup, ForceLocation, ForceCollocation>;
        using TColumns = TableColumns<TenantName, SlotType, DataCenter, CollocationGroup, ForceLocation, ForceCollocation, Count>;
    };

    struct BannedSlots : Table<6> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SlotId : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<NodeId, SlotId>;
        using TColumns = TableColumns<NodeId, SlotId>;
    };

    struct PinnedSlots : Table<7> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct SlotId : Column<2, NScheme::NTypeIds::Utf8> {};
        struct TenantName : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Label : Column<4, NScheme::NTypeIds::Utf8> {};
        struct Deadline : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId, SlotId>;
        using TColumns = TableColumns<NodeId, SlotId, TenantName, Label, Deadline>;
    };

    using TTables = SchemaTables<Config, RequiredSlots, Slots, SlotLabels, SlotsAllocations, BannedSlots, PinnedSlots>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                     ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

} // NTenantSlotBroker
} // NKikimr
