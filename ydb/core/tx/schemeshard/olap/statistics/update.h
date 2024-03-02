#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/constructor.h>

namespace NKikimr::NSchemeShard {

    class TOlapStatisticsUpsert {
    private:
        YDB_READONLY_DEF(TString, Name);
    protected:
        NOlap::NStatistics::TConstructorContainer Constructor;
    public:
        TOlapStatisticsUpsert() = default;

        const NOlap::NStatistics::TConstructorContainer& GetConstructor() const {
            return Constructor;
        }

        bool DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TConstructorContainer& requestedProto);
        void SerializeToProto(NKikimrColumnShardStatisticsProto::TConstructorContainer& requestedProto) const;
    };

    class TOlapStatisticsModification {
    private:
        YDB_READONLY_DEF(TVector<TOlapStatisticsUpsert>, Upsert);
        YDB_READONLY_DEF(TSet<TString>, Drop);
    public:
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
    };
}
