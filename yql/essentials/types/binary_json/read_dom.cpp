#include "read.h"

#include <yql/essentials/minikql/dom/node.h>

#include <util/generic/vector.h>

namespace NKikimr::NBinaryJson {

using namespace NUdf;
using namespace NYql::NDom;

TUnboxedValue ReadElementToJsonDom(const TEntryCursor& cursor, const NUdf::IValueBuilder* valueBuilder) {
    switch (cursor.GetType()) {
        case EEntryType::BoolFalse:
            return MakeBool(false);
        case EEntryType::BoolTrue:
            return MakeBool(true);
        case EEntryType::Null:
            return MakeEntity();
        case EEntryType::String:
            return SetUtf8Mark(MakeString(cursor.GetString(), valueBuilder));
        case EEntryType::Number:
            return MakeDouble(cursor.GetNumber());
        case EEntryType::Container:
            return ReadContainerToJsonDom(cursor.GetContainer(), valueBuilder);
    }
}

TUnboxedValue ReadContainerToJsonDom(const TContainerCursor& cursor, const NUdf::IValueBuilder* valueBuilder) {
    switch (cursor.GetType()) {
        case EContainerType::TopLevelScalar: {
            return ReadElementToJsonDom(cursor.GetElement(0), valueBuilder);
        }
        case EContainerType::Array: {
            TVector<TUnboxedValue> items;
            items.reserve(cursor.GetSize());

            auto it = cursor.GetArrayIterator();
            while (it.HasNext()) {
                const auto element = ReadElementToJsonDom(it.Next(), valueBuilder);
                items.push_back(element);
            }
            return MakeList(items.data(), items.size(), valueBuilder);
        }
        case EContainerType::Object: {
            TVector<TPair> items;
            items.reserve(cursor.GetSize());

            auto it = cursor.GetObjectIterator();
            while (it.HasNext()) {
                const auto [sourceKey, sourceValue] = it.Next();
                auto key = ReadElementToJsonDom(sourceKey, valueBuilder);
                auto value = ReadElementToJsonDom(sourceValue, valueBuilder);
                items.emplace_back(std::move(key), std::move(value));
            }
            return MakeDict(items.data(), items.size());
        }
    }
}

TUnboxedValue ReadToJsonDom(const TBinaryJson& binaryJson, const NUdf::IValueBuilder* valueBuilder) {
    return ReadToJsonDom(TStringBuf(binaryJson.Data(), binaryJson.Size()), valueBuilder);
}

TUnboxedValue ReadToJsonDom(TStringBuf binaryJson, const NUdf::IValueBuilder* valueBuilder) {
    auto reader = TBinaryJsonReader::Make(binaryJson);
    return ReadContainerToJsonDom(reader->GetRootCursor(), valueBuilder);
}

} // namespace NKikimr::NBinaryJson
