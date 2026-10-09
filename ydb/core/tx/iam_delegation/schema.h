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

    struct Secrets : Table<2> {
        struct Incarnation : Column<1, NScheme::NTypeIds::String> {};
        struct OwnerId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct LocalId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Data : Column<4, NScheme::NTypeIds::String> {};
        using TKey = TableKey<Incarnation, OwnerId, LocalId>;
        using TColumns = TableColumns<Incarnation, OwnerId, LocalId, Data>;
    };

    struct Delegations : Table<3> {
        struct OperationId : Column<1, NScheme::NTypeIds::String> {};
        struct Data : Column<2, NScheme::NTypeIds::String> {};
        struct OriginalStage : Column<3, NScheme::NTypeIds::String> {};
        using TKey = TableKey<OperationId>;
        using TColumns = TableColumns<OperationId, Data, OriginalStage>;
    };

    // Referrers and operation IDs are never recycled, including after revocation.
    struct Referrers : Table<4> {
        struct ReferrerId : Column<1, NScheme::NTypeIds::String> {};
        struct OperationId : Column<2, NScheme::NTypeIds::String> {};
        using TKey = TableKey<ReferrerId>;
        using TColumns = TableColumns<ReferrerId, OperationId>;
    };

    // The creating intent owns the name until authoritative retirement.
    struct Names : Table<5> {
        struct Incarnation : Column<1, NScheme::NTypeIds::String> {};
        struct SecretPath : Column<2, NScheme::NTypeIds::String> {};
        struct OperationId : Column<3, NScheme::NTypeIds::String> {};
        using TKey = TableKey<Incarnation, SecretPath>;
        using TColumns = TableColumns<Incarnation, SecretPath, OperationId>;
    };

    // A covering index provides full pages without reading every database row.
    struct Inventory : Table<6> {
        struct Incarnation : Column<1, NScheme::NTypeIds::String> {};
        struct OperationId : Column<2, NScheme::NTypeIds::String> {};
        struct Data : Column<3, NScheme::NTypeIds::String> {};
        using TKey = TableKey<Incarnation, OperationId>;
        using TColumns = TableColumns<Incarnation, OperationId, Data>;
    };

    struct Revocations : Table<7> {
        struct DueAtUs : Column<1, NScheme::NTypeIds::Uint64> {};
        struct OperationId : Column<2, NScheme::NTypeIds::String> {};
        using TKey = TableKey<DueAtUs, OperationId>;
        using TColumns = TableColumns<DueAtUs, OperationId>;
    };

    using TTables = SchemaTables<Databases, Secrets, Delegations, Referrers, Names, Inventory, Revocations>;
};

} // namespace NKikimr::NIamDelegation
