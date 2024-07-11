#include "operator.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/columnshard/splitter/abstract/chunks.h>

namespace NKikimr::NOlap::NStatistics::NVariability {

class IValuesContainer {
protected:
    std::optional<arrow::Type::type> DataType;
    ui32 DifferentCount = 0;

    virtual void DoAddArray(const std::shared_ptr<arrow::Array>& array) = 0;
public:
    virtual ~IValuesContainer() = default;
    ui32 GetDifferentCount() const {
        return DifferentCount;
    }

    void AddArray(const std::shared_ptr<arrow::Array>& array) {
        if (!DataType) {
            DataType = array->type_id();
        } else {
            AFL_VERIFY(DataType == array->type_id())("base", (ui32)*DataType)("to", (ui32)array->type_id());
        }
        return DoAddArray(array);
    }
};

template <class TArrowElement>
class TCTypeValuesContainer: public IValuesContainer {
private:
    using TWrap = TArrowElement;
    using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
    using TCType = typename TWrap::T::c_type;
    using TCContainer = THashSet<TCType>;

    TCContainer ElementsStorage;
protected:
    virtual void DoAddArray(const std::shared_ptr<arrow::Array>& array) override {
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<TWrap, TArrowElement>) {
                const TArray& arrTyped = static_cast<const TArray&>(*array);
                for (ui32 i = 0; i < array->length(); ++i) {
                    if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                        if (ElementsStorage.emplace(arrTyped.Value(i)).second) {
                            ++DifferentCount;
                        }
                        continue;
                    }
                    AFL_VERIFY(false);
                }
                return true;
            }
            AFL_VERIFY(false);
            return false;
        });
    }
};

template <class TArrowElement>
class TStringValuesContainer: public IValuesContainer {
private:
    using TWrap = TArrowElement;
    using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
    using TCType = TString;
    using TCContainer = THashSet<TCType>;

    TCContainer ElementsStorage;
protected:
    virtual void DoAddArray(const std::shared_ptr<arrow::Array>& array) override {
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            if constexpr (std::is_same_v<TWrap, TArrowElement>) {
                const TArray& arrTyped = static_cast<const TArray&>(*array);
                for (ui32 i = 0; i < array->length(); ++i) {
                    if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                        auto value = arrTyped.GetView(i);
                        if (ElementsStorage.emplace(value.data(), value.size()).second) {
                            ++DifferentCount;
                        }
                        continue;
                    }
                    AFL_VERIFY(false);
                }
                return true;
            }
            AFL_VERIFY(false);
            return false;
        });
    }
};

class TDifferentElementsAggregator {
private:
    std::shared_ptr<IValuesContainer> Container;
public:
    TDifferentElementsAggregator() = default;

    bool HasData() const {
        return !!Container;
    }

    ui32 GetDifferentCount() const {
        return Container ? Container->GetDifferentCount() : 0;
    }

    void AddArray(const std::shared_ptr<arrow::Array>& array) {
        if (!Container) {
            NArrow::SwitchType(array->type_id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if (!Container) {
                    if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                        Container = std::make_shared<TCTypeValuesContainer<TWrap>>();
                        Container->AddArray(array);
                        return true;
                    }
                    if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                        Container = std::make_shared<TStringValuesContainer<TWrap>>();
                        Container->AddArray(array);
                        return true;
                    }
                    AFL_VERIFY(false);
                }
                return false;
            });
        }
        Container->AddArray(array);
    }
};

void TOperator::DoFillStatisticsData(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, TPortionStorage& portionStats, const IIndexInfo& index) const {
    auto it = data.find(EntityId);
    AFL_VERIFY(it != data.end());
    auto loader = index.GetColumnLoaderVerified(EntityId);
    std::shared_ptr<arrow::Scalar> result;
    TDifferentElementsAggregator aggregator;
    for (auto&& i : it->second) {
        auto rb = NArrow::TStatusValidator::GetValid(loader->Apply(i->GetData()));
        AFL_VERIFY(rb->num_columns() == 1);
        aggregator.AddArray(rb->column(0));
    }
    AFL_VERIFY(aggregator.HasData());
    portionStats.AddScalar(std::make_shared<arrow::UInt32Scalar>(aggregator.GetDifferentCount()));
}

bool TOperator::DoDeserializeFromProto(const NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) {
    if (!proto.HasVariability()) {
        return false;
    }
    EntityId = proto.GetVariability().GetEntityId();
    if (!EntityId) {
        return false;
    }
    return true;
}

void TOperator::DoSerializeToProto(NKikimrColumnShardStatisticsProto::TOperatorContainer& proto) const {
    AFL_VERIFY(EntityId);
    proto.MutableVariability()->SetEntityId(EntityId);
}

}