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

    using TTables = SchemaTables<Databases>;
};

} // namespace NKikimr::NIamDelegation
