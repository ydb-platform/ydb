#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorSchema : NIceDb::Schema {
    struct State : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct TrackingConfig : Column<2, NScheme::NTypeIds::String> {};
        struct CurrentHeartbeat : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, TrackingConfig, CurrentHeartbeat>;
    };

    struct Sources : Table<2> {
        struct DataShardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LastHeartbeat : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<DataShardId>;
        using TColumns = TableColumns<DataShardId, LastHeartbeat>;
    };

    using TTables = SchemaTables<State, Sources>;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>>;
};

} // namespace NKikimr::NCdcAggregator
