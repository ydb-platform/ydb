#include "data_extractor.h"
#include "direct_builder.h"
#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TSimdBuffers: public TDataBuilder::IBuffers {
private:
    std::vector<simdjson::padded_string> PaddedStrings;
    std::vector<TString> Strings;

public:
    TSimdBuffers(std::vector<simdjson::padded_string>&& paddedStrings, std::vector<TString>&& strings)
        : PaddedStrings(std::move(paddedStrings))
        , Strings(std::move(strings)) {
    }
};

TConclusionStatus TJsonScanExtractor::DoAddDataToBuilders(const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const {
    auto arr = std::static_pointer_cast<arrow::BinaryArray>(sourceArray);
    // std::optional<bool> isBinaryJson;
    // if (arr->type()->id() == arrow::binary()->id()) {
    //     isBinaryJson = false;
    // }
    AFL_VERIFY(arr->type()->id() == arrow::binary()->id());
    if (!arr->length()) {
        return TConclusionStatus::Success();
    }
#if 0
    simdjson::ondemand::parser simdParser;
    std::vector<simdjson::padded_string> paddedStrings;
    std::vector<TString> forceSIMDStrings;
    ui32 sumBuf = 0;
    ui32 paddedBorder = 0;
    for (i32 i = arr->length() - 1; i >= 1; --i) {
        sumBuf += arr->GetView(i).size();
        if (sumBuf > simdjson::SIMDJSON_PADDING) {
            paddedBorder = i;
            break;
        }
    }
#endif
    for (ui32 i = 0; i < arr->length(); ++i) {
        const auto view = arr->GetView(i);
        if (view.size() && !arr->IsNull(i)) {
            TStringBuf sbJson(view.data(), view.size());
#if 0
            if (ForceSIMDJsonParsing) {
                TString json = NBinaryJson::SerializeToJson(sbJson);
                forceSIMDStrings.emplace_back(json);
                sbJson = TStringBuf(json.data(), json.size());
                std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
                simdjson::simdjson_result<simdjson::ondemand::document> doc;
                if (i < paddedBorder) {
                    doc = simdParser.iterate(
                        simdjson::padded_string_view(sbJson.data(), sbJson.size(), sbJson.size() + simdjson::SIMDJSON_PADDING));
                } else {
                    paddedStrings.emplace_back(simdjson::padded_string(sbJson.data(), sbJson.size()));
                    doc = simdParser.iterate(paddedStrings.back());
                }
                auto conclusion = TSIMDExtractor(doc, FirstLevelOnly).Fill(dataBuilder, iterators);
                if (conclusion.IsFail()) {
                    return conclusion;
                }
            } else {
#endif
                auto reader = NBinaryJson::TBinaryJsonReader::Make(sbJson);
                auto cursor = reader->GetRootCursor();
                std::deque<std::unique_ptr<IJsonObjectExtractor>> iterators;
                if (cursor.GetType() == NBinaryJson::EContainerType::Object) {
                    iterators.push_back(std::make_unique<TKVExtractor>(cursor.GetObjectIterator(), TStringBuf(), FirstLevelOnly));
                }
                // TODO: add support for arrays and scalars if needed
                // } else if (cursor.GetType() == NBinaryJson::EContainerType::Array) {
                //     iterators.push_back(std::make_unique<TArrayExtractor>(cursor.GetArrayIterator(), TStringBuf(), FirstLevelOnly));
                // }
                while (iterators.size()) {
                    const auto conclusion = iterators.front()->Fill(dataBuilder, iterators);
                    if (conclusion.IsFail()) {
                        return conclusion;
                    }
                    iterators.pop_front();
                }
#if 0
            }
#endif
        }
        dataBuilder.StartNextRecord();
    }

#if 0
    if (paddedStrings.size()) {
        dataBuilder.StoreBuffer(std::make_shared<TSimdBuffers>(std::move(paddedStrings), std::move(forceSIMDStrings))); // ??
    }
#endif
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
