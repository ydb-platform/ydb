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
    std::variant<NBinaryJson::TBinaryJson, TString> res;
    bool addRes = true;

    if (value.GetType() == NBinaryJson::EEntryType::String) {
        NJson::TJsonValue jsonValue(value.GetString());
        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&jsonValue);
        res = NBinaryJson::SerializeToBinaryJson(sout.Str(), false);
    } else if (value.GetType() == NBinaryJson::EEntryType::Number) {
        const double val = value.GetNumber();
        double integer;
        if (modf(val, &integer)) {
            res = NBinaryJson::SerializeToBinaryJson(std::to_string(val), false);
        } else {
            res = NBinaryJson::SerializeToBinaryJson(std::to_string((i64)integer), false);
        }
    } else if (value.GetType() == NBinaryJson::EEntryType::BoolFalse) {
        static const TString falseString = "false";
        res = NBinaryJson::SerializeToBinaryJson(falseString, false);
    } else if (value.GetType() == NBinaryJson::EEntryType::BoolTrue) {
        static const TString trueString = "true";
        res = NBinaryJson::SerializeToBinaryJson(trueString, false);
    } else if (value.GetType() == NBinaryJson::EEntryType::Container) {
        auto container = value.GetContainer();
        if (FirstLevelOnly || container.GetType() == NBinaryJson::EContainerType::Array) {
            res = NBinaryJson::SerializeToBinaryJson(NBinaryJson::SerializeToJson(container), false);
        // TODO: add support for arrays if needed
        // } else if (container.GetType() == NBinaryJson::EContainerType::Array) {
        //     iterators.emplace_back(std::make_unique<TArrayExtractor>(container.GetArrayIterator(), key));
        //     addRes = false;
        } else if (container.GetType() == NBinaryJson::EContainerType::Object) {
            iterators.emplace_back(std::make_unique<TKVExtractor>(container.GetObjectIterator(), key));
            addRes = false;
        } else {
            return TConclusionStatus::Fail("unexpected top value scalar in container iterator");
        }

    } else if (value.GetType() == NBinaryJson::EEntryType::Null) {
        static const TString nullString = "null";
        res = NBinaryJson::SerializeToBinaryJson(nullString, false);
    } else {
        return TConclusionStatus::Fail("unexpected json value type: " + ::ToString((int)value.GetType()));
    }

    if (addRes) {
        if (const TString* val = std::get_if<TString>(&res)) {
            return TConclusionStatus::Fail(*val);
        } else if (const NBinaryJson::TBinaryJson* resBinaryJson = std::get_if<NBinaryJson::TBinaryJson>(&res)) {
            dataBuilder.AddKV(key, *resBinaryJson);
        } else {
            return TConclusionStatus::Fail("undefined case for binary json construction");
        }
    }

    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
