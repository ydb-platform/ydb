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
    };

    inline TString HashForValue(const TString& value) {
        uint64_t data[2];
        data[0] = t1ha2_atonce128(&data[1], value.data(), value.size(), 1);
        return HexEncode(data, sizeof(data));
    }

} // NKikimr::NTestShard
