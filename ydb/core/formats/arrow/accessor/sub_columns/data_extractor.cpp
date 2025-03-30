#include "data_extractor.h"
#include "direct_builder.h"
#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TFirstLevelSchemaData::DoAddDataToBuilders(
    const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    if (sourceArray->type()->id() != arrow::binary()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    auto arr = std::static_pointer_cast<arrow::StringArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.size() && !arr->IsNull(i)) {
            auto reader = NBinaryJson::TBinaryJsonReader::Make(TStringBuf(view.data(), view.size()));
            auto cursor = reader->GetRootCursor();
            std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
            if (cursor.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.push_back(std::make_unique<TKVExtractor>(cursor.GetObjectIterator(), TStringBuf(), FirstLevelOnly));
            } else if (cursor.GetType() == NBinaryJson::EContainerType::Array) {
                iterators.push_back(std::make_unique<TArrayExtractor>(cursor.GetArrayIterator(), TStringBuf(), FirstLevelOnly));
            }
            while (iterators.size()) {
                const auto conclusion = iterators.front()->Fill(dataBuilder, iterators);
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                iterators.pop_front();
            }
        }
        dataBuilder.StartNextRecord();
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IDataAdapter::AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    return DoAddDataToBuilders(sourceArray, dataBuilder);
}

TDataAdapterContainer TDataAdapterContainer::GetDefault() {
    static TDataAdapterContainer result(std::make_shared<NSubColumns::TFirstLevelSchemaData>());
    return result;
}

TConclusionStatus TSIMDJsonExtractor::DoAddDataToBuilders(
    const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    if (sourceArray->type()->id() != arrow::binary()->id() && sourceArray->type()->id() != arrow::utf8()->id()) {
        return TConclusionStatus::Fail("incorrect base type for subcolumns schema usage");
    }

    auto arr = std::static_pointer_cast<arrow::StringArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.size() && !arr->IsNull(i)) {
            std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
            try {
                auto doc = dataBuilder.ParseJsonOnDemand(std::string_view(view.data(), view.size()));
                auto conclusion = TSIMDExtractor(doc, FirstLevelOnly).Fill(dataBuilder, iterators);
                if (conclusion.IsFail()) {
                    return conclusion;
                }
            } catch (const simdjson::simdjson_error& e) {
                return TConclusionStatus::Fail("cannot parse json exception: " + TString(e.what()));
            }
        }
        dataBuilder.StartNextRecord();
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
