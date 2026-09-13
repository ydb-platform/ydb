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
            Database
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
            DirectoryPath
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

    // One active (or last applied) CDC schema barrier per replicated target.
    // The schema is stored as the normalized protobuf supplied by workers.  It
    // is deliberately kept separate from Targets: a barrier must survive a
    // controller restart without changing the target lifecycle state.
    struct SchemaBarriers: Table<7> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct Phase: Column<3, NScheme::NTypeIds::Uint8> {};
        struct Schema: Column<4, NScheme::NTypeIds::String> {};
        struct DstAlterTxId: Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ReplicationId, TargetId>;
        using TColumns = TableColumns<ReplicationId, TargetId, Phase, Schema, DstAlterTxId>;
    };

    // This is the immutable membership snapshot used by a schema barrier.
    // A worker is marked reported only after its report has been durably
    // compared with the barrier schema.
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

    // The complete target-only flush snapshot for a global-consistency
    // schema barrier.  Entries intentionally remain after a successful
    // target flush: replaying CommitWrites is idempotent and lets recovery
    // resume safely without guessing which proposal result was persisted.
    struct SchemaBarrierFlushes: Table<9> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        struct WriteTxId: Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ReplicationId, TargetId, WriteTxId>;
        using TColumns = TableColumns<ReplicationId, TargetId, WriteTxId>;
    };

    // A completed DescribeTopic snapshot. Schema barriers may only snapshot
    // workers after this marker is durable, never from a partial stream of
    // asynchronous TEvRunWorker registrations.
    struct WorkerSnapshots: Table<10> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        struct TargetId: Column<2, NScheme::NTypeIds::Uint64> {};
        using TKey = TableKey<ReplicationId, TargetId>;
        using TColumns = TableColumns<ReplicationId, TargetId>;
    };

    // Configuration/state alterations accepted while a schema barrier is
    // degraded.  The requested values already live in Replications; this row
    // is the durable instruction to re-enter the target alter lifecycle once
    // every barrier for the replication has become safe.
    struct DeferredAlters: Table<11> {
        struct ReplicationId: Column<1, NScheme::NTypeIds::Uint64> {};
        using TKey = TableKey<ReplicationId>;
        using TColumns = TableColumns<ReplicationId>;
    };

    using TTables = SchemaTables<
        SysParams,
        Replications,
        Targets,
        SrcStreams,
        TxIds,
        Workers,
        SchemaBarriers,
        SchemaBarrierWorkers,
        SchemaBarrierFlushes,
        WorkerSnapshots,
        DeferredAlters
    >;

}; // TControllerSchema

}
