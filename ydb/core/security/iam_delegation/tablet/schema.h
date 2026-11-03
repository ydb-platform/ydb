#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NIamDelegation {

struct TSchema : NIceDb::Schema {
    struct Databases : Table<1> {
        struct Incarnation : Column<1, NScheme::NTypeIds::String> {};
        struct Data : Column<2, NScheme::NTypeIds::String> {};
        using TKey = TableKey<Incarnation>;
        using TColumns = TableColumns<Incarnation, Data>;
    };

    // IDs2..8 are reserved for the independently published delegation lifecycle.
    struct ServiceBackoff : Table<9> {
        struct Key : Column<1, NScheme::NTypeIds::String> {};
        struct Data : Column<2, NScheme::NTypeIds::String> {};
        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Data>;
    };

    using TTables = SchemaTables<Databases, ServiceBackoff>;
};

} // namespace NKikimr::NIamDelegation
