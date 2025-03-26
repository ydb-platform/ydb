#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TArrayExtractor::DoFill(TDataBuilder& dataBuilder, std::deque<std::shared_ptr<IJsonObjectExtractor>>& iterators) {
    ui32 idx = 0;
    while (Iterator.HasNext()) {
        auto value = Iterator.Next();
        const TString jsonKey = "[" + ::ToString(idx++) + "]";
        const TStringBuf key = dataBuilder.AddKeyOwn(GetPrefix(), jsonKey);
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
                iterators.emplace_back(std::make_shared<TArrayExtractor>(Storage, container.GetArrayIterator(), GetPrefixWithOwn(jsonKey)));
            } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.emplace_back(std::make_shared<TKVExtractor>(Storage, container.GetObjectIterator(), GetPrefixWithOwn(jsonKey)));
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
                iterators.emplace_back(
                    std::make_shared<TArrayExtractor>(Storage, container.GetArrayIterator(), GetPrefixWith(jsonKey.GetString())));
            } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
                iterators.emplace_back(std::make_shared<TKVExtractor>(Storage, container.GetObjectIterator(), GetPrefixWith(jsonKey.GetString())));
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
