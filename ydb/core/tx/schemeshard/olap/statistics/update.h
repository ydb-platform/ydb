#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKikimr::NSchemeShard {

    class TOlapMultiColumnStatisticsUpsert {
    private:
        YDB_READONLY_DEF(TString, Name);
        YDB_READONLY_DEF(TVector<TString>, ColumnNames);
        YDB_READONLY_DEF(TVector<NKikimrSchemeOp::EMultiColumnStatisticsType>, Types);
    public:
        TOlapMultiColumnStatisticsUpsert() = default;

        void DeserializeFromProto(const NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto);
    };

    class TOlapMultiColumnStatisticsUpdate {
    private:
        YDB_READONLY_DEF(TVector<TOlapMultiColumnStatisticsUpsert>, UpsertMultiColumnStatistics);
        YDB_READONLY_DEF(TSet<TString>, DropMultiColumnStatistics);
    public:
        // Build the update from a full table description (CREATE TABLE): every declared statistics is an upsert.
        bool Parse(const NKikimrSchemeOp::TColumnTableDescription& description, IErrorCollector& errors);
        // Build the update from an alter diff (ALTER TABLE ADD/DROP STATISTICS).
        bool Parse(const NKikimrSchemeOp::TAlterColumnTable& alterRequest, IErrorCollector& errors);
    };
}
