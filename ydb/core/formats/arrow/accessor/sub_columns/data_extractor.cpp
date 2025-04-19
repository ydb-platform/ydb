#include "data_extractor.h"
#include "direct_builder.h"
#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TJsonScanExtractor::DoAddDataToBuilders(
    const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const {
    auto arr = std::static_pointer_cast<arrow::BinaryArray>(sourceArray);
    std::optional<bool> isBinaryJson;
    if (arr->type()->id() == arrow::utf8()->id()) {
        isBinaryJson = false;
    }
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.size() && !arr->IsNull(i)) {
            TStringBuf sbJson(view.data(), view.size());
            if (!isBinaryJson) {
                isBinaryJson = NBinaryJson::IsValidBinaryJson(sbJson);
            }
            TString json;
            if (*isBinaryJson && ForceSIMDJsonParsing) {
                json = NBinaryJson::SerializeToJson(sbJson);
                sbJson = TStringBuf(json.data(), json.size());
            }
            if (!json && *isBinaryJson) {
                auto reader = NBinaryJson::TBinaryJsonReader::Make(sbJson);
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
            } else {
                std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
                auto doc = dataBuilder.ParseJsonOnDemand(sbJson);
                auto conclusion = TSIMDExtractor(doc, FirstLevelOnly).Fill(dataBuilder, iterators);
                if (conclusion.IsFail()) {
                    return conclusion;
                }
            }
        }
        dataBuilder.StartNextRecord();
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IDataAdapter::AddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept {
    try {
        return DoAddDataToBuilders(sourceArray, dataBuilder);
    } catch (...) {
        return TConclusionStatus::Fail("exception on data extraction: " + CurrentExceptionMessage());
    }
}

TDataAdapterContainer TDataAdapterContainer::GetDefault() {
    static TDataAdapterContainer result(std::make_shared<NSubColumns::TJsonScanExtractor>());
    return result;
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
