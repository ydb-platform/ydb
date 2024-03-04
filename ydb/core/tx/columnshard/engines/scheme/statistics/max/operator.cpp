#include "operator.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap::NStatistics::NMax {

void TOperator::DoFillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const TIndexInfo& index) const {
    AFL_VERIFY(data.size() == 1);
    auto loader = index.GetColumnLoaderVerified(EntityId);
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : data.begin()->second) {
        auto rb = NArrow::TStatusValidator::GetValid(loader->Apply(i->GetData()));
        AFL_VERIFY(rb->num_columns() == 1);
        auto res = NArrow::FindMinMaxPosition(rb->column(0));
        auto currentScalarMax = NArrow::TStatusValidator::GetValid(rb->column(0)->GetScalar(res.second));
        if (!result || NArrow::ScalarCompare(result, currentScalarMax) < 0) {
            result = currentScalarMax;
        }
    }
    portionStats.AddScalar(result);
}

bool TOperator::DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) {
    if (!proto.HasMax()) {
        return false;
    }
    EntityId = proto.GetMax().GetEntityId();
    if (!EntityId) {
        return false;
    }
    return true;
}

void TOperator::DoSerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const {
    AFL_VERIFY(EntityId);
    proto.MutableMax()->SetEntityId(EntityId);
}

}