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

    using TTables = SchemaTables<
                            State
                            >;

    using TSettings = SchemaSettings<
                                    ExecutorLogBatching<true>,
                                    ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
                                    >;
};

} // NGraph
} // NKikimr
