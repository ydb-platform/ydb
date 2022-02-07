#pragma once

#include "defs.h"

namespace NKikimr::NTestShard {

    struct Schema : NIceDb::Schema {
        struct State : Table<100> {
            struct Key : Column<1, NScheme::NTypeIds::Bool> { static constexpr Type Default = {}; };
            struct Settings : Column<2, NScheme::NTypeIds::String> {};
            struct Digest : Column<3, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, Settings, Digest>;
        };

        using TTables = SchemaTables<State>;
    };

} // NKikimr::NTestShard
