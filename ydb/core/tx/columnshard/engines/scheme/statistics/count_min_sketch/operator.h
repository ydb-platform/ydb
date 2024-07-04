#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/operator.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

namespace NKikimr::NOlap::NStatistics::NCountMinSketch {

class TOperator: public IOperator {
private:
    using TBase = IOperator;
    ui32 EntityId = 0;
    static inline auto Registrator = TFactory::TRegistrator<TOperator>(::ToString(EType::CountMinSketch));
protected:
    virtual void DoCopyData(const TPortionStorageCursor& cursor, const TPortionStorage& portionStatsFrom, TPortionStorage& portionStatsTo) const override {
        std::shared_ptr<arrow::Scalar> scalar = portionStatsFrom.GetScalarVerified(cursor);
        portionStatsTo.AddScalar(scalar);
    }

    virtual void DoFillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const IIndexInfo& index) const override;
    virtual void DoShiftCursor(TPortionStorageCursor& cursor) const override {
        cursor.AddScalarsPosition(1);
    }
    virtual std::vector<ui32> GetEntityIds() const override {
        return {EntityId};
    }
    virtual bool DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const override;
public:

    static bool IsAvailableType(const NScheme::TTypeInfo) {
        return true;
    }

    TOperator()
        : TBase(EType::CountMinSketch)
    {

    }

    TOperator(const ui32 entityId)
        : TBase(EType::CountMinSketch)
        , EntityId(entityId) {

    }
};

}
