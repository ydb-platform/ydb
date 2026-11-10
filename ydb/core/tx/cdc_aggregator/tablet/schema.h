#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/cdc_aggregator/stream/stream.h>

namespace NKikimr::NCdcAggregator {

struct TCdcAggregatorSchema : NIceDb::Schema {
    static constexpr ui32 SingleRowId = 1;

    // Absent until the tablet is configured.
    struct Config : Table<1> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        // Serialized NKikimrCdcAggregator::TStreamConfig.
        struct StreamConfig : Column<2, NScheme::NTypeIds::String> {};
        struct Phase : Column<3, NScheme::NTypeIds::Uint8> {
            using Type = EStreamPhase;
            static constexpr EStreamPhase Default = EStreamPhase::Configured;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, StreamConfig, Phase>;
    };

    // Current set of sources. Step, TxId and Payload are null until the first
    // report of the source.
    struct Sources : Table<2> {
        struct SourceId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Step : Column<2, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Payload : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SourceId>;
        using TColumns = TableColumns<SourceId, Step, TxId, Payload>;
    };

    // Absent until every source has reported.
    struct ReadyBarrier : Table<3> {
        struct Id : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Step : Column<2, NScheme::NTypeIds::Uint64> {};
        struct TxId : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Payload : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Step, TxId, Payload>;
    };

    // One row per child of every replaced source.
    struct SourceReplacements : Table<4> {
        struct ParentId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct ChildId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct OperationId : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<ParentId, ChildId>;
        using TColumns = TableColumns<ParentId, ChildId, OperationId>;
    };

    using TTables = SchemaTables<
        Config,
        Sources,
        ReadyBarrier,
        SourceReplacements>;

    // Reports of many sources arrive concurrently: batch their commits,
    // flushing the log at least every 500 us.
    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<500>>;
};

} // namespace NKikimr::NCdcAggregator
