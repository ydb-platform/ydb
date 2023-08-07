#pragma once

#include "defs.h"

namespace NKikimr::NTestShard {

    struct Schema : NIceDb::Schema {
        struct State : Table<100> {
            struct Key : Column<1, NScheme::NTypeIds::Bool> { static constexpr Type Default = {}; };
            struct Settings : Column<2, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Settings>;
        };

        using TTables = SchemaTables<State>;

        struct EmptySettings {
            static void Materialize(NIceDb::TToughDb&) {}
        };

        using TSettings = SchemaSettings<EmptySettings>;
    };

} // NKikimr::NTestShard
