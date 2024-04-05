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

    struct BaseStats : Table<2> {
        struct SchemeShardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Stats         : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<SchemeShardId>;
        using TColumns = TableColumns<SchemeShardId, Stats>;
    };

    struct Statistics : Table<3> {
        struct ColumnTag      : Column<1, NScheme::NTypeIds::Uint32> {};
        struct CountMinSketch : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ColumnTag>;
        using TColumns = TableColumns<ColumnTag, CountMinSketch>;
    };

    using TTables = SchemaTables<
        SysParams,
        BaseStats,
        Statistics
    >;

    using TSettings = SchemaSettings<
        ExecutorLogBatching<true>,
        ExecutorLogFlushPeriod<512>
    >;

    static constexpr ui64 SysParam_Database = 1;
    static constexpr ui64 SysParam_StartKey = 2;
    static constexpr ui64 SysParam_ScanTableOwnerId = 3;
    static constexpr ui64 SysParam_ScanTableLocalPathId = 4;
};

} // NKikimr::NStat
