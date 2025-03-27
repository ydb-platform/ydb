#include "data_extractor.h"
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
            //        NBinaryJson::TBinaryJson bJson(view.data(), view.size());
            //        auto bJson = NBinaryJson::SerializeToBinaryJson(TStringBuf(view.data(), view.size()));
            //        const NBinaryJson::TBinaryJson* bJsonParsed = std::get_if<NBinaryJson::TBinaryJson>(&bJson);
            //        AFL_VERIFY(bJsonParsed)("error", *std::get_if<TString>(&bJson))("json", TStringBuf(view.data(), view.size()));
            //        const NBinaryJson::TBinaryJson* bJsonParsed = &bJson;
            auto reader = NBinaryJson::TBinaryJsonReader::Make(TStringBuf(view.data(), view.size()));
            auto cursor = reader->GetRootCursor();
            std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
            if (cursor.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.push_back(std::make_unique<TKVExtractor>(cursor.GetObjectIterator(), TStringBuf()));
            } else if (cursor.GetType() == NBinaryJson::EContainerType::Array) {
                iterators.push_back(std::make_unique<TArrayExtractor>(cursor.GetArrayIterator(), TStringBuf()));
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

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
