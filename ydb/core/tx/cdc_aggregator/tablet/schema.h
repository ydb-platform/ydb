#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

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

        using TKey = TableKey<DataShardId>;
        using TColumns = TableColumns<DataShardId, LastHeartbeatStep, LastHeartbeatTxId>;
    };

    struct Heartbeats : Table<3> {
        struct VersionStep : Column<1, NScheme::NTypeIds::Uint64> {};
        struct VersionTxId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Data : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<VersionStep, VersionTxId>;
        using TColumns = TableColumns<VersionStep, VersionTxId, Data>;
    };

    using TTables = SchemaTables<State, Sources, Heartbeats>;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>>;
};

} // namespace NKikimr::NCdcAggregator
