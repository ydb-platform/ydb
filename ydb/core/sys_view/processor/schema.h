#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NSysView {

struct TProcessorSchema : NIceDb::Schema {
    struct SysParams : Table<1> {
        struct Id    : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Value>;
    };

    struct IntervalSummaries : Table<2> {
        struct QueryHash : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Index     : Column<2, NScheme::NTypeIds::Uint32> {};
        struct CPU       : Column<3, NScheme::NTypeIds::Uint64> {};
        struct NodeId    : Column<4, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<QueryHash, Index>;
        using TColumns = TableColumns<QueryHash, Index, CPU, NodeId>;
    };

    struct IntervalMetrics : Table<3> {
        struct QueryHash : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Metrics   : Column<2, NScheme::NTypeIds::String> {};
        struct Text      : Column<3, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<QueryHash>;
        using TColumns = TableColumns<QueryHash, Metrics, Text>;
    };

    struct IntervalTops : Table<4> {
        struct TypeCol   : Column<1, NScheme::NTypeIds::Uint32> {
            static TString GetColumnName(const TString&) { return "Type"; }
        };
        struct QueryHash : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Value     : Column<3, NScheme::NTypeIds::Uint64> {};
        struct NodeId    : Column<4, NScheme::NTypeIds::Uint32> {};
        struct Stats     : Column<5, NScheme::NTypeIds::String> {};

        using TKey = TableKey<TypeCol, QueryHash>;
        using TColumns = TableColumns<TypeCol, QueryHash, Value, NodeId, Stats>;
    };

    struct NodesToRequest : Table<5> {
        struct NodeId      : Column<1, NScheme::NTypeIds::Uint32> {};
        struct QueryHashes : Column<2, NScheme::NTypeIds::String> {};
        struct TextsToGet  : Column<3, NScheme::NTypeIds::String> {};
        struct ByDuration  : Column<4, NScheme::NTypeIds::String> {};
        struct ByReadBytes : Column<5, NScheme::NTypeIds::String> {};
        struct ByCpuTime   : Column<6, NScheme::NTypeIds::String> {};
        struct ByRequestUnits : Column<7, NScheme::NTypeIds::String> {};

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, QueryHashes, TextsToGet,
            ByDuration, ByReadBytes, ByCpuTime, ByRequestUnits>;
    };

#define RESULT_QUERY_TABLE(TableName, TableID)                           \
    struct TableName : Table<TableID> {                                  \
        struct IntervalEnd : Column<1, NScheme::NTypeIds::Timestamp> {}; \
        struct Rank        : Column<2, NScheme::NTypeIds::Uint32> {};    \
        struct Text        : Column<3, NScheme::NTypeIds::Utf8> {};      \
        struct Data        : Column<4, NScheme::NTypeIds::String> {};    \
                                                                         \
        using TKey = TableKey<IntervalEnd, Rank>;                        \
        using TColumns = TableColumns<IntervalEnd, Rank, Text, Data>;    \
    };

    RESULT_QUERY_TABLE(MetricsOneMinute, 6)
    RESULT_QUERY_TABLE(MetricsOneHour, 7)
    RESULT_QUERY_TABLE(TopByDurationOneMinute, 8)
    RESULT_QUERY_TABLE(TopByDurationOneHour, 9)
    RESULT_QUERY_TABLE(TopByReadBytesOneMinute, 10)
    RESULT_QUERY_TABLE(TopByReadBytesOneHour, 11)
    RESULT_QUERY_TABLE(TopByCpuTimeOneMinute, 12)
    RESULT_QUERY_TABLE(TopByCpuTimeOneHour, 13)
    RESULT_QUERY_TABLE(TopByRequestUnitsOneMinute, 14)
    RESULT_QUERY_TABLE(TopByRequestUnitsOneHour, 15)

#undef RESULT_QUERY_TABLE

    struct IntervalPartitionTops : Table<16> {
        struct TypeCol  : Column<1, NScheme::NTypeIds::Uint32> {
            static TString GetColumnName(const TString&) { return "Type"; }
        };
        struct TabletId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Data     : Column<3, NScheme::NTypeIds::String> {};

        using TKey = TableKey<TypeCol, TabletId>;
        using TColumns = TableColumns<TypeCol, TabletId, Data>;
    };

#define RESULT_PARTITION_TABLE(TableName, TableID)                       \
    struct TableName : Table<TableID> {                                  \
        struct IntervalEnd : Column<1, NScheme::NTypeIds::Timestamp> {}; \
        struct Rank        : Column<2, NScheme::NTypeIds::Uint32> {};    \
        struct Data        : Column<3, NScheme::NTypeIds::String> {};    \
                                                                         \
        using TKey = TableKey<IntervalEnd, Rank>;                        \
        using TColumns = TableColumns<IntervalEnd, Rank, Data>;          \
    };

    RESULT_PARTITION_TABLE(TopPartitionsOneMinute, 17)
    RESULT_PARTITION_TABLE(TopPartitionsOneHour, 18)

#undef RESULT_PARTITION_TABLE

    using TTables = SchemaTables<
        SysParams,
        IntervalSummaries,
        IntervalMetrics,
        NodesToRequest,
        IntervalTops,
        MetricsOneMinute,
        MetricsOneHour,
        TopByDurationOneMinute,
        TopByDurationOneHour,
        TopByReadBytesOneMinute,
        TopByReadBytesOneHour,
        TopByCpuTimeOneMinute,
        TopByCpuTimeOneHour,
        TopByRequestUnitsOneMinute,
        TopByRequestUnitsOneHour,
        IntervalPartitionTops,
        TopPartitionsOneMinute,
        TopPartitionsOneHour
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>
    >;

    static constexpr ui64 SysParam_Database = 1;
    static constexpr ui64 SysParam_CurrentStage = 2;
    static constexpr ui64 SysParam_IntervalEnd = 3;
};

}
}
