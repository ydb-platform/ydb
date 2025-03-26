#include "data_extractor.h"

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
            std::deque<std::shared_ptr<IJsonObjectExtractor>> iterators;
            if (cursor.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.push_back(std::make_shared<TKVExtractor>(cursor.GetObjectIterator(), std::vector<TStringBuf>()));
            } else if (cursor.GetType() == NBinaryJson::EContainerType::Array) {
                iterators.push_back(std::make_shared<TArrayExtractor>(cursor.GetArrayIterator(), std::vector<TStringBuf>()));
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

TConclusionStatus TArrayExtractor::DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) {
    ui32 idx = 0;
    while (Iterator.HasNext()) {
        auto value = Iterator.Next();
        const TStringBuf key = dataBuilder.AddKeyOwn(GetPrefix(), "[" + ::ToString(idx++) + "]");
        if (value.GetType() == NBinaryJson::EEntryType::String) {
            dataBuilder.AddKV(key, value.GetString());
        } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
            dataBuilder.AddKVOwn(key, ::ToString(value.GetNumber()));
        } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
            dataBuilder.AddKVOwn(key, "0");
        } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
            dataBuilder.AddKVOwn(key, "1");
        } else if (value.GetType() == NBinaryJson::EEntryType::Container) {
            auto container = value.GetContainer();
            if (container.GetType() == NBinaryJson::EContainerType::Array) {
                iterators.emplace_back(std::make_shared<TArrayExtractor>(container.GetArrayIterator(), GetPrefixWith(key)));
            } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.emplace_back(std::make_shared<TKVExtractor>(container.GetObjectIterator(), GetPrefixWith(key)));
            } else {
                return TConclusionStatus::Fail("unexpected top value scalar in container iterator");
            }

        } else if (value.GetType() == NBinaryJson::EEntryType::Null) {
            dataBuilder.AddKVNull(key);
        } else {
            return TConclusionStatus::Fail("unexpected json value type: " + ::ToString((int)value.GetType()));
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TKVExtractor::DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) {
    while (Iterator.HasNext()) {
        auto [jsonKey, value] = Iterator.Next();
        if (jsonKey.GetType() != NBinaryJson::EEntryType::String) {
            continue;
        }
        const TStringBuf key = dataBuilder.AddKey(GetPrefix(), jsonKey.GetString());
        if (value.GetType() == NBinaryJson::EEntryType::String) {
            dataBuilder.AddKV(key, value.GetString());
        } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
            dataBuilder.AddKVOwn(key, ::ToString(value.GetNumber()));
        } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
            dataBuilder.AddKVOwn(key, "0");
        } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
            dataBuilder.AddKVOwn(key, "1");
        } else if (value.GetType() == NBinaryJson::EEntryType::Container) {
            auto container = value.GetContainer();
            if (container.GetType() == NBinaryJson::EContainerType::Array) {
                iterators.emplace_back(std::make_shared<TArrayExtractor>(container.GetArrayIterator(), GetPrefixWith(key)));
            } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.emplace_back(std::make_shared<TKVExtractor>(container.GetObjectIterator(), GetPrefixWith(key)));
            } else {
                return TConclusionStatus::Fail("unexpected top value scalar in container iterator");
            }

        } else if (value.GetType() == NBinaryJson::EEntryType::Null) {
            dataBuilder.AddKVNull(key);
        } else {
            return TConclusionStatus::Fail("unexpected json value type: " + ::ToString((int)value.GetType()));
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
