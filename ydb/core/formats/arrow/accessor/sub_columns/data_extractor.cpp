#include "data_extractor.h"
#include "direct_builder.h"
#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TJsonScanExtractor::DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const {
    AFL_VERIFY(sourceArray->type()->id() == arrow::binary()->id())
              ("sourceArray->type()->id()", (int)sourceArray->type()->id());
    if (!sourceArray->length()) {
        return TConclusionStatus::Success();
    }

    auto arr = std::static_pointer_cast<arrow::BinaryArray>(sourceArray);
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.empty() || arr->IsNull(i)) {
            dataBuilder.StartNextRecord();
            continue;
        }

        TStringBuf sbJson(view.data(), view.size());
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
