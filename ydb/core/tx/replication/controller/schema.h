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
        struct DesiredState: Column<8, NScheme::NTypeIds::Uint8> { using Type = TReplication::EState; };
        struct Database: Column<9, NScheme::NTypeIds::Utf8> {};
        struct DeferredAlter: Column<10, NScheme::NTypeIds::Bool> {
            static constexpr bool Default = false;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<
            Id,
            PathOwnerId,
            PathLocalId,
            Config,
            State,
            Issue,
            NextTargetId,
            DesiredState,
            Database,
            DeferredAlter
        >;
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
        struct TransformLambda: Column<10, NScheme::NTypeIds::Utf8> {}; // Deprecated. Remove in next major release (27-1).
        struct RunAsUser: Column<11, NScheme::NTypeIds::Utf8> {}; // Deprecated. Remove in next major release (27-1).
        struct DirectoryPath: Column<12, NScheme::NTypeIds::Utf8> {}; // Deprecated. Remove in next major release (27-1).
        // Marks that registration has completed for the target, so Workers is
        // a complete partition-membership set rather than a partial stream of
        // asynchronous registrations.
        struct WorkerSetComplete: Column<13, NScheme::NTypeIds::Bool> {
            static constexpr bool Default = false;
        };
        struct SchemaBarrierPhase: Column<14, NScheme::NTypeIds::Uint8> {
            static constexpr ui8 Default = 0; // No schema barrier.
        };
        struct SchemaBarrierChange: Column<15, NScheme::NTypeIds::String> {};
        // Only the DDL transaction for the active schema barrier is tracked
        // here; ordinary destination alters do not use this column.
        struct DstAlterTxId: Column<16, NScheme::NTypeIds::Uint64> {
            static constexpr ui64 Default = 0;
        };

        using TKey = TableKey<ReplicationId, Id>;
        using TColumns = TableColumns<
            ReplicationId,
            Id,
            Kind,
            SrcPath,
            DstPath,
            DstState,
            DstPathOwnerId,
            DstPathLocalId,
            Issue,
            TransformLambda,
            RunAsUser,
            DirectoryPath,
            WorkerSetComplete,
            SchemaBarrierPhase,
            SchemaBarrierChange,
            DstAlterTxId
        >;
    };

    struct SrcStreams: Table<4> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct Name: Column<3, NScheme::NTypeIds::Utf8> {};
        struct State: Column<4, NScheme::NTypeIds::Uint8> { using Type = TReplication::EStreamState; };
        struct ConsumerName: Column<5, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<ReplicationId, TargetId>;
        using TColumns = TableColumns<ReplicationId, TargetId, Name, State, ConsumerName>;
    };

    struct TxIds: Table<5> {
        struct VersionStep: Column<1, NScheme::NTypeIds::Uint64> {};
        struct VersionTxId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct WriteTxId: Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<VersionStep, VersionTxId>;
        using TColumns = TableColumns<VersionStep, VersionTxId, WriteTxId>;
    };

    struct Workers: Table<6> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct WorkerId: Column<3, NScheme::NTypeIds::Uint64> {};
        struct HeartbeatVersionStep: Column<4, NScheme::NTypeIds::Uint64> {};
        struct HeartbeatVersionTxId: Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ReplicationId, TargetId, WorkerId>;
        using TColumns = TableColumns<ReplicationId, TargetId, WorkerId, HeartbeatVersionStep, HeartbeatVersionTxId>;
    };

    // Freeze membership at the first schema report until every partition has
    // crossed the same record. These rows outlive the live Workers rows.
    struct SchemaBarrierWorkers: Table<8> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct WorkerId: Column<3, NScheme::NTypeIds::Uint64> {};
        struct Reported: Column<4, NScheme::NTypeIds::Bool> {};
        struct Applied: Column<5, NScheme::NTypeIds::Bool> {};
        struct Completed: Column<6, NScheme::NTypeIds::Bool> {};
        struct Offset: Column<7, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ReplicationId, TargetId, WorkerId>;
        using TColumns = TableColumns<ReplicationId, TargetId, WorkerId, Reported, Applied, Completed, Offset>;
    };

    using TTables = SchemaTables<
        SysParams,
        Replications,
        Targets,
        SrcStreams,
        TxIds,
        Workers,
        SchemaBarrierWorkers
    >;

}; // TControllerSchema

}
