#include "json_extractors.h"

#include <util/string/split.h>
#include <util/string/vector.h>
#include <yql/essentials/types/binary_json/format.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <math.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TConclusionStatus TArrayExtractor::DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) {
    ui32 idx = 0;
    while (Iterator.HasNext()) {
        auto value = Iterator.Next();
        const TStringBuf key = dataBuilder.AddKeyOwn(GetPrefix(), "[" + std::to_string(idx++) + "]");
        auto conclusion = AddDataToBuilder(dataBuilder, iterators, key, value);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TKVExtractor::DoFill(TDataBuilder& dataBuilder, std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators) {
    while (Iterator.HasNext()) {
        auto [jsonKey, value] = Iterator.Next();
        if (jsonKey.GetType() != NBinaryJson::EEntryType::String) {
            continue;
        }
        const TStringBuf key = dataBuilder.AddKey(GetPrefix(), jsonKey.GetString());
        auto conclusion = AddDataToBuilder(dataBuilder, iterators, key, value);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus IJsonObjectExtractor::AddDataToBuilder(TDataBuilder& dataBuilder,
    std::deque<std::unique_ptr<IJsonObjectExtractor>>& iterators, const TStringBuf key, NBinaryJson::TEntryCursor& value) const {
    if (value.GetType() == NBinaryJson::EEntryType::String) {
        dataBuilder.AddKV(key, value.GetString());
    } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
        const double val = value.GetNumber();
        double integer;
        if (modf(val, &integer)) {
            dataBuilder.AddKVOwn(key, std::to_string(val));
        } else {
            dataBuilder.AddKVOwn(key, std::to_string((i64)integer));
        }
    } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
        static const TString zeroString = "0";
        dataBuilder.AddKV(key, TStringBuf(zeroString.data(), zeroString.size()));
    } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
        static const TString oneString = "1";
        dataBuilder.AddKV(key, TStringBuf(oneString.data(), oneString.size()));
    } else if (value.GetType() == NBinaryJson::EEntryType::Container) {
        auto container = value.GetContainer();
        if (FirstLevelOnly) {
            dataBuilder.AddKVOwn(key, NBinaryJson::SerializeToJson(container));
        } else if (container.GetType() == NBinaryJson::EContainerType::Array) {
            iterators.emplace_back(std::make_unique<TArrayExtractor>(container.GetArrayIterator(), key));
        } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
            iterators.emplace_back(std::make_unique<TKVExtractor>(container.GetObjectIterator(), key));
        } else {
            return TConclusionStatus::Fail("unexpected top value scalar in container iterator");
        }

    } else if (value.GetType() == NBinaryJson::EEntryType::Null) {
        dataBuilder.AddKVNull(key);
    } else {
        return TConclusionStatus::Fail("unexpected json value type: " + ::ToString((int)value.GetType()));
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
