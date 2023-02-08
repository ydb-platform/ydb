#pragma once

#include "replication.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NReplication::NController {

struct TControllerSchema: NIceDb::Schema {
    struct SysParams: Table<1> {
        struct Id: Column<1, NScheme::NTypeIds::Uint32> {};
        struct IntValue: Column<2, NScheme::NTypeIds::Uint64> {};
        struct TextValue: Column<3, NScheme::NTypeIds::Utf8> {};
        struct BinaryValue: Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, IntValue, TextValue, BinaryValue>;
    };

    struct Replications: Table<2> {
        struct Id: Column<1, NScheme::NTypeIds::Uint64> {};
        struct PathOwnerId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct PathLocalId: Column<3, NScheme::NTypeIds::Uint64> {};
        struct Config: Column<4, NScheme::NTypeIds::String> {};
        struct State: Column<5, NScheme::NTypeIds::Uint8> { using Type = TReplication::EState; };
        struct Issue: Column<6, NScheme::NTypeIds::Utf8> {};
        struct NextTargetId: Column<7, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 1; };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, PathOwnerId, PathLocalId, Config, State, Issue, NextTargetId>;
    };

    struct Targets: Table<3> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct Id: Column<2, NScheme::NTypeIds::Uint64> {};
        struct Kind: Column<3, NScheme::NTypeIds::Uint8> { using Type = TReplication::ETargetKind; };
        struct SrcPath: Column<4, NScheme::NTypeIds::Utf8> {};
        struct DstPath: Column<5, NScheme::NTypeIds::Utf8> {};
        struct DstState: Column<6, NScheme::NTypeIds::Uint8> { using Type = TReplication::EDstState; };
        struct DstPathOwnerId: Column<7, NScheme::NTypeIds::Uint64> {
            using Type = TOwnerId;
            static constexpr Type Default = InvalidOwnerId;
        };
        struct DstPathLocalId: Column<8, NScheme::NTypeIds::Uint64> {
            using Type = TLocalPathId;
            static constexpr Type Default = InvalidLocalPathId;
        };
        struct Issue: Column<9, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<ReplicationId, Id>;
        using TColumns = TableColumns<ReplicationId, Id, Kind, SrcPath, DstPath, DstState, DstPathOwnerId, DstPathLocalId, Issue>;
    };

    struct SrcStreams: Table<4> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct Name: Column<3, NScheme::NTypeIds::Utf8> {};
        struct State: Column<4, NScheme::NTypeIds::Uint8> { using Type = TReplication::EStreamState; };

        using TKey = TableKey<ReplicationId, TargetId>;
        using TColumns = TableColumns<ReplicationId, TargetId, Name, State>;
    };

    using TTables = SchemaTables<
        SysParams,
        Replications,
        Targets,
        SrcStreams
    >;

}; // TControllerSchema

}
