#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorSchema : NIceDb::Schema {
    struct State : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> { static constexpr Type Value = 0; };
        struct TrackingConfig : Column<2, NScheme::NTypeIds::String> {};
        struct CurrentHeartbeatStep : Column<3, NScheme::NTypeIds::Uint64> {};
        struct CurrentHeartbeatTxId : Column<4, NScheme::NTypeIds::Uint64> {};
        struct CurrentHeartbeatData : Column<5, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, TrackingConfig, CurrentHeartbeatStep, CurrentHeartbeatTxId, CurrentHeartbeatData>;
    };

    struct Sources : Table<2> {
        struct DataShardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LastHeartbeatStep : Column<2, NScheme::NTypeIds::Uint64> {};
        struct LastHeartbeatTxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct LastHeartbeatData : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<DataShardId>;
        using TColumns = TableColumns<DataShardId, LastHeartbeatStep, LastHeartbeatTxId, LastHeartbeatData>;
    };

    struct PartitioningChanges : Table<4> {
        struct ParentDataShardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct OperationId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct ChildDataShardIds : Column<3, NScheme::NTypeIds::String> { using Type = TVector<ui64>; };

        using TKey = TableKey<ParentDataShardId, OperationId>;
        using TColumns = TableColumns<ParentDataShardId, OperationId, ChildDataShardIds>;
    };

    using TTables = SchemaTables<State, Sources, PartitioningChanges>;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>>;
};

} // namespace NKikimr::NCdcAggregator
