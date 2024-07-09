#include "operator.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>
#include <ydb/core/util/count_min_sketch.h>


namespace NKikimr::NOlap::NStatistics::NCountMinSketch {

class TCountMinSketchAggregator {
private:
    std::unique_ptr<TCountMinSketch> Sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());

public:
    TCountMinSketchAggregator() = default;

    bool HasData() const {
        return !!Sketch->GetElementCount();
    }

    std::shared_ptr<arrow::FixedSizeBinaryScalar> GetSketchAsScalar() {
        auto sketchAsBuf = Sketch->AsStringBuf();
        auto b = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(sketchAsBuf.Data()), sketchAsBuf.Size());
        auto type = std::make_shared<arrow::FixedSizeBinaryType>(Sketch->GetSize());
        return std::make_shared<arrow::FixedSizeBinaryScalar>(b, type);
    }

    void AddArray(const std::shared_ptr<arrow::Array>& array) {
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;

            const TArray& arrTyped = static_cast<const TArray&>(*array);
            for (ui32 i = 0; i < array->length(); ++i) {
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    auto cell = TCell::Make(arrTyped.Value(i));
                    Sketch->Count(cell.Data(), cell.Size());
                    continue;
                }
                if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                    auto view = arrTyped.GetView(i);
                    Sketch->Count(view.data(), view.size());
                    continue;
                }
                AFL_VERIFY(false);
            }
            return true;
        });
    }
};

void TOperator::DoFillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const IIndexInfo& index) const {
    auto loader = index.GetColumnLoaderVerified(EntityId);
    auto it = data.find(EntityId);
    AFL_VERIFY(it != data.end());
    TCountMinSketchAggregator aggregator;
    for (auto&& i : it->second) {
        auto rb = NArrow::TStatusValidator::GetValid(loader->Apply(i->GetData()));
        AFL_VERIFY(rb->num_columns() == 1);
        aggregator.AddArray(rb->column(0));
    }
    AFL_VERIFY(aggregator.HasData());
    portionStats.AddScalar(aggregator.GetSketchAsScalar());
}

bool TOperator::DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) {
    if (!proto.HasCountMinSketch()) {
        return false;
    }
    EntityId = proto.GetCountMinSketch().GetEntityId();
    if (!EntityId) {
        return false;
    }
    return true;
}

void TOperator::DoSerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const {
    AFL_VERIFY(EntityId);
    proto.MutableCountMinSketch()->SetEntityId(EntityId);
}

}
