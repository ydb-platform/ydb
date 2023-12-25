#pragma once
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NGraph {

struct Schema : NIceDb::Schema {
    struct State : Table<1> {
        struct Name : Column<1, NScheme::NTypeIds::Text> {};
        struct ValueUI64 : Column<2, NScheme::NTypeIds::Uint64> {};
        struct ValueText : Column<3, NScheme::NTypeIds::Text> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<Name, ValueUI64, ValueText>;
    };

    struct MetricsIndex : Table<2> {
        struct Name : Column<1, NScheme::NTypeIds::Text> {};
        struct Id : Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<Name, Id>;
    };

    struct MetricsValues : Table<3> {
        struct Timestamp : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Id : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<3, NScheme::NTypeIds::Double> {};

        using TKey = TableKey<Timestamp, Id>;
        using TColumns = TableColumns<Timestamp, Id, Value>;
    };

    using TTables = SchemaTables<
                            State,
                            MetricsIndex,
                            MetricsValues
                            >;

    using TSettings = SchemaSettings<
                                    ExecutorLogBatching<true>,
                                    ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
                                    >;
};

} // NGraph
} // NKikimr
