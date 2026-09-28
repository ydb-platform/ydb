#pragma once

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NUdfStore {

struct TCompileControllerSchema : NIceDb::Schema {
    struct SysParams : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Value>;
    };

    //! Which platforms this tenant has and how much each node can take. Kept in
    //! local DB so that a leader that has just moved still knows which
    //! `artifacts/{cpu_spec}` tables to reconcile before anybody re-registers.
    //! Liveness (heartbeat, inflight) is deliberately not persisted: it says
    //! nothing after a leader change.
    struct Workers : Table<2> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct CpuSpec : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Capacity : Column<3, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, CpuSpec, Capacity>;
    };

    //! Outstanding exclusive rights to compile. Survives a leader change so
    //! that a worker re-confirming an assignment id in its heartbeat can be
    //! matched against what was actually handed out.
    struct Assignments : Table<3> {
        struct Name : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Kind : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Uid : Column<3, NScheme::NTypeIds::Utf8> {};
        struct CpuSpec : Column<4, NScheme::NTypeIds::Utf8> {};
        struct AssignmentId : Column<5, NScheme::NTypeIds::Uint64> {};
        struct NodeId : Column<6, NScheme::NTypeIds::Uint32> {};
        struct DeadlineSeconds : Column<7, NScheme::NTypeIds::Uint64> {};
        struct Generation : Column<8, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Name, Kind, Uid, CpuSpec>;
        using TColumns = TableColumns<
            Name, Kind, Uid, CpuSpec, AssignmentId, NodeId, DeadlineSeconds, Generation>;
    };

    //! Consecutive genuine failures per gap. Keyed by uid as well, so a new
    //! upload starts from a clean slate without any admin action.
    struct Attempts : Table<4> {
        struct Name : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Kind : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Uid : Column<3, NScheme::NTypeIds::Utf8> {};
        struct CpuSpec : Column<4, NScheme::NTypeIds::Utf8> {};
        struct FailCount : Column<5, NScheme::NTypeIds::Uint32> {};
        struct LastError : Column<6, NScheme::NTypeIds::Utf8> {};
        struct Poisoned : Column<7, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Name, Kind, Uid, CpuSpec>;
        using TColumns = TableColumns<
            Name, Kind, Uid, CpuSpec, FailCount, LastError, Poisoned>;
    };

    using TTables = SchemaTables<SysParams, Workers, Assignments, Attempts>;
    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;

    enum class ESysParam : ui64 {
        NextAssignmentId = 1,
    };
};

} // namespace NKikimr::NUdfStore
