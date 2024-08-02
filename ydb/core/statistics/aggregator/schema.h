#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NStat {

struct TAggregatorSchema : NIceDb::Schema {
    struct SysParams : Table<1> {
        struct Id    : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Value>;
    };

    struct BaseStatistics : Table<2> {
        struct SchemeShardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Stats         : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SchemeShardId>;
        using TColumns = TableColumns<SchemeShardId, Stats>;
    };

    struct ColumnStatistics : Table<3> {
        struct ColumnTag      : Column<1, NScheme::NTypeIds::Uint32> {};
        struct CountMinSketch : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ColumnTag>;
        using TColumns = TableColumns<ColumnTag, CountMinSketch>;
    };

    struct ScheduleTraversals : Table<4> {
        struct OwnerId        : Column<1, NScheme::NTypeIds::Uint64> {};
        struct LocalPathId    : Column<2, NScheme::NTypeIds::Uint64> {};
        struct LastUpdateTime : Column<3, NScheme::NTypeIds::Timestamp> {};
        struct SchemeShardId  : Column<4, NScheme::NTypeIds::Uint64> {};
        struct IsColumnTable  : Column<5, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<OwnerId, LocalPathId>;
        using TColumns = TableColumns<
            OwnerId,
            LocalPathId,
            LastUpdateTime,
            SchemeShardId,
            IsColumnTable
        >;
    };

    struct ForceTraversals : Table<5> {
        struct OperationId    : Column<1, NScheme::NTypeIds::Uint64> {};
        struct OwnerId        : Column<2, NScheme::NTypeIds::Uint64> {};
        struct LocalPathId    : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Cookie         : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OperationId, OwnerId, LocalPathId>;
        using TColumns = TableColumns<
            OperationId,
            OwnerId,
            LocalPathId,
            Cookie
        >;
    };

    using TTables = SchemaTables<
        SysParams,
        BaseStatistics,
        ColumnStatistics,
        ScheduleTraversals,
        ForceTraversals
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>
    >;

    static constexpr ui64 SysParam_Database = 1;
    static constexpr ui64 SysParam_TraversalStartKey = 2;
    static constexpr ui64 SysParam_ForceTraversalOperationId = 3;
    static constexpr ui64 SysParam_ForceTraversalCookie = 4;
    static constexpr ui64 SysParam_TraversalTableOwnerId = 5;
    static constexpr ui64 SysParam_TraversalTableLocalPathId = 6;
    static constexpr ui64 SysParam_TraversalStartTime = 7;
    static constexpr ui64 SysParam_NextForceTraversalOperationId = 8;    
    static constexpr ui64 SysParam_TraversalIsColumnTable = 9;
    static constexpr ui64 SysParam_GlobalTraversalRound = 10;
};

} // NKikimr::NStat
