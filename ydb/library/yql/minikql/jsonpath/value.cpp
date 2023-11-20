#include "value.h"

#include <ydb/library/yql/minikql/dom/node.h>

namespace NYql::NJsonPath {

using namespace NUdf;
using namespace NDom;
using namespace NKikimr;
using namespace NKikimr::NBinaryJson;

TArrayIterator::TArrayIterator()
    : Iterator(TEmptyMarker())
{
}

TArrayIterator::TArrayIterator(const TUnboxedValue& iterator)
    : Iterator(iterator)
{
}

TArrayIterator::TArrayIterator(TUnboxedValue&& iterator)
    : Iterator(std::move(iterator))
{
}

TArrayIterator::TArrayIterator(const NBinaryJson::TArrayIterator& iterator)
    : Iterator(iterator)
{
}

TArrayIterator::TArrayIterator(NBinaryJson::TArrayIterator&& iterator)
    : Iterator(std::move(iterator))
{
}

bool TArrayIterator::Next(TValue& value) {
    if (std::holds_alternative<TEmptyMarker>(Iterator)) {
        return false;
    } else if (auto* iterator = std::get_if<NBinaryJson::TArrayIterator>(&Iterator)) {
        if (!iterator->HasNext()) {
            return false;
        }
        value = TValue(iterator->Next());
        return true;
    } else if (auto* iterator = std::get_if<TUnboxedValue>(&Iterator)) {
        TUnboxedValue result;
        const bool success = iterator->Next(result);
        if (success) {
            value = TValue(result);
        }
        return success;
    } else {
        Y_ABORT("Unexpected variant case in Next");
    }
}

TObjectIterator::TObjectIterator()
    : Iterator(TEmptyMarker())
{
}

TObjectIterator::TObjectIterator(const TUnboxedValue& iterator)
    : Iterator(iterator)
{
}

TObjectIterator::TObjectIterator(TUnboxedValue&& iterator)
    : Iterator(std::move(iterator))
{
}

TObjectIterator::TObjectIterator(const NBinaryJson::TObjectIterator& iterator)
    : Iterator(iterator)
{
}

TObjectIterator::TObjectIterator(NBinaryJson::TObjectIterator&& iterator)
    : Iterator(std::move(iterator))
{
}

bool TObjectIterator::Next(TValue& key, TValue& value) {
    if (std::holds_alternative<TEmptyMarker>(Iterator)) {
        return false;
    } else if (auto* iterator = std::get_if<NBinaryJson::TObjectIterator>(&Iterator)) {
        if (!iterator->HasNext()) {
            return false;
        }
        const auto [itKey, itValue] = iterator->Next();
        key = TValue(itKey);
        value = TValue(itValue);
        return true;
    } else if (auto* iterator = std::get_if<TUnboxedValue>(&Iterator)) {
        TUnboxedValue itKey;
        TUnboxedValue itValue;
        const bool success = iterator->NextPair(itKey, itValue);
        if (success) {
            key = TValue(itKey);
            value = TValue(itValue);
        }
        return success;
    } else {
        Y_ABORT("Unexpected variant case in Next");
    }
}

TValue::TValue()
    : Value(MakeEntity())
{
}

TValue::TValue(const TUnboxedValue& value)
    : Value(value)
{
}

TValue::TValue(TUnboxedValue&& value)
    : Value(std::move(value))
{
}

TValue::TValue(const TEntryCursor& value)
    : Value(value)
{
    UnpackInnerValue();
}

TValue::TValue(TEntryCursor&& value)
    : Value(std::move(value))
{
    UnpackInnerValue();
}

TValue::TValue(const TContainerCursor& value)
    : Value(value)
{
    UnpackInnerValue();
}

TValue::TValue(TContainerCursor&& value)
    : Value(std::move(value))
{
    UnpackInnerValue();
}

EValueType TValue::GetType() const {
    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        switch (value->GetType()) {
            case EEntryType::BoolFalse:
            case EEntryType::BoolTrue:
                return EValueType::Bool;
            case EEntryType::Null:
                return EValueType::Null;
            case EEntryType::Number:
                return EValueType::Number;
            case EEntryType::String:
                return EValueType::String;
            case EEntryType::Container:
                Y_ABORT("Logical error: TEntryCursor with Container type must be converted to TContainerCursor");
        }
    } else if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        switch (value->GetType()) {
            case EContainerType::Array:
                return EValueType::Array;
            case EContainerType::Object:
                return EValueType::Object;
            case EContainerType::TopLevelScalar:
                Y_ABORT("Logical error: TContainerCursor with TopLevelScalar type must be converted to TEntryCursor");
        }
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        switch (GetNodeType(*value)) {
            case ENodeType::Bool:
                return EValueType::Bool;
            case ENodeType::Double:
            case ENodeType::Int64:
            case ENodeType::Uint64:
                return EValueType::Number;
            case ENodeType::Dict:
            case ENodeType::Attr:
                return EValueType::Object;
            case ENodeType::List:
                return EValueType::Array;
            case ENodeType::String:
                return EValueType::String;
            case ENodeType::Entity:
                return EValueType::Null;
        }
    } else {
        Y_ABORT("Unexpected variant case in GetType");
    }
}

bool TValue::Is(EValueType type) const {
    return GetType() == type;
}

bool TValue::IsBool() const {
    return Is(EValueType::Bool);
}

bool TValue::IsNumber() const {
    return Is(EValueType::Number);
}

bool TValue::IsString() const {
    return Is(EValueType::String);
}

bool TValue::IsNull() const {
    return Is(EValueType::Null);
}

bool TValue::IsObject() const {
    return Is(EValueType::Object);
}

bool TValue::IsArray() const {
    return Is(EValueType::Array);
}

double TValue::GetNumber() const {
    Y_DEBUG_ABORT_UNLESS(IsNumber());

    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        return value->GetNumber();
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        if (IsNodeType(*value, ENodeType::Double)) {
            return value->Get<double>();
        } else if (IsNodeType(*value, ENodeType::Int64)) {
            return static_cast<double>(value->Get<i64>());
        } else {
            return static_cast<double>(value->Get<ui64>());
        }
    } else {
        Y_ABORT("Unexpected variant case in GetNumber");
    }
}

bool TValue::GetBool() const {
    Y_DEBUG_ABORT_UNLESS(IsBool());

    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        return value->GetType() == EEntryType::BoolTrue;
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        return value->Get<bool>();
    } else {
        Y_ABORT("Unexpected variant case in GetBool");
    }
}

const TStringBuf TValue::GetString() const {
    Y_DEBUG_ABORT_UNLESS(IsString());

    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        return value->GetString();
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        return value->AsStringRef();
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

ui32 TValue::GetSize() const {
    Y_DEBUG_ABORT_UNLESS(IsArray() || IsObject());

    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        return value->GetSize();
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        if (value->IsEmbedded()) {
            return 0;
        }

        if (IsNodeType(*value, ENodeType::List)) {
            return value->GetListLength();
        } else {
            return value->GetDictLength();
        }
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

TValue TValue::GetElement(ui32 index) const {
    Y_DEBUG_ABORT_UNLESS(IsArray());

    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        return TValue(value->GetElement(index));
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        return TValue(value->GetElement(index));
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

TArrayIterator TValue::GetArrayIterator() const {
    Y_DEBUG_ABORT_UNLESS(IsArray());

    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        return TArrayIterator(value->GetArrayIterator());
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        if (value->IsEmbedded()) {
            return TArrayIterator();
        }
        return TArrayIterator(value->GetListIterator());
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

TMaybe<TValue> TValue::Lookup(const TStringBuf key) const {
    Y_DEBUG_ABORT_UNLESS(IsObject());

    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        const auto payload = value->Lookup(key);
        if (!payload.Defined()) {
            return Nothing();
        }
        return TValue(*payload);
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        if (value->IsEmbedded()) {
            return Nothing();
        }

        // Lookup on TUnboxedValue can be performed only with TUnboxedValue key.
        // To avoid allocating new string we use our custom Lookup method defined
        // on underlying TMapNode that accepts TStringRef
        const auto* dict = static_cast<const TMapNode*>(value->AsBoxed().Get());
        if (const auto payload = dict->Lookup(key)) {
            return {TValue(payload)};
        } else {
            return Nothing();
        }
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

TObjectIterator TValue::GetObjectIterator() const {
    Y_DEBUG_ABORT_UNLESS(IsObject());

    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        return TObjectIterator(value->GetObjectIterator());
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        if (value->IsEmbedded()) {
            return TObjectIterator();
        }
        return TObjectIterator(value->GetDictIterator());
    } else {
        Y_ABORT("Unexpected variant case in GetString");
    }
}

TUnboxedValue TValue::ConvertToUnboxedValue(const NUdf::IValueBuilder* valueBuilder) const {
    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        return ReadElementToJsonDom(*value, valueBuilder);
    } else if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        return ReadContainerToJsonDom(*value, valueBuilder);
    } else if (const auto* value = std::get_if<TUnboxedValue>(&Value)) {
        return *value;
    } else {
        Y_ABORT("Unexpected variant case in ConvertToUnboxedValue");
    }
}

void TValue::UnpackInnerValue() {
    // If TEntryCursor points to container, we need to extract TContainerCursor
    if (const auto* value = std::get_if<TEntryCursor>(&Value)) {
        if (value->GetType() == EEntryType::Container) {
            Value = value->GetContainer();
        }
    }

    // If TContainerCursor points to top level scalar, we need to extract TEntryCursor
    if (const auto* value = std::get_if<TContainerCursor>(&Value)) {
        if (value->GetType() == EContainerType::TopLevelScalar) {
            Value = value->GetElement(0);
        }
    }
}

}

