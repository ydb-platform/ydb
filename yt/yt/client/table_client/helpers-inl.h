#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "row_buffer.h"
#include "row_batch.h"

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <array>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////
// Scalar inline types

#define XX(T) \
    template <> \
    struct TUnversionedValueConversionTraits<T, void> \
    { \
        static constexpr bool Scalar = true; \
        static constexpr bool Inline = true; \
    };

XX(std::nullopt_t)
XX(i64)
XX(ui64)
XX(i32)
XX(ui32)
XX(i16)
XX(ui16)
XX(i8)
XX(ui8)
XX(bool)
XX(double)
XX(TInstant)
XX(TDuration)

#undef XX

////////////////////////////////////////////////////////////////////////////////
// Scalar non-inline types

#define XX(T) \
    template <> \
    struct TUnversionedValueConversionTraits<T, void> \
    { \
        static constexpr bool Scalar = true; \
        static constexpr bool Inline = false; \
    };

XX(TString)
XX(TStringBuf)
XX(std::string)
XX(TGuid)

#undef XX

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires TEnumTraits<T>::IsEnum
struct TUnversionedValueConversionTraits<T>
{
    static constexpr bool Scalar = true;
    static constexpr bool Inline = !TEnumTraits<T>::IsStringSerializableEnum;
};

template <class T>
struct TUnversionedValueConversionTraits<std::optional<T>>
{
    static constexpr bool Scalar = TUnversionedValueConversionTraits<T>::Scalar;
    static constexpr bool Inline = TUnversionedValueConversionTraits<T>::Inline;
};

template <class T, class TTag>
struct TUnversionedValueConversionTraits<TStrongTypedef<T, TTag>>
{
    static constexpr bool Scalar = TUnversionedValueConversionTraits<T>::Scalar;
    static constexpr bool Inline = TUnversionedValueConversionTraits<T>::Inline;
};

template <class T>
struct TUnversionedValueConversionTraits<TAnnotatedValue<T>>
{
    static constexpr bool Scalar = TUnversionedValueConversionTraits<T>::Scalar;
    static constexpr bool Inline = TUnversionedValueConversionTraits<T>::Inline;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TAnnotatedValue<T>& value,
    const TRowBufferPtr& rowBuffer,
    int /*id*/,
    EValueFlags /*flags*/)
{
    ToUnversionedValue(unversionedValue, value.Value, rowBuffer, value.Id, value.Flags);
}

template <class T>
struct TValueWithIdTrait
{
    static constexpr bool WithId = false;
};

template <class T>
struct TValueWithIdTrait<TAnnotatedValue<T>>
{
    static constexpr bool WithId = true;
};

template <class... Ts>
struct TRowValueTypesChecker
{
    static constexpr bool AllWithIds = (... && TValueWithIdTrait<Ts>::WithId);
    static constexpr bool AllWithoutIds = (... && !TValueWithIdTrait<Ts>::WithId);
    static_assert(AllWithIds || AllWithoutIds, "All values must be wrapped into TValueWithId or none must be.");
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires TEnumTraits<T>::IsEnum
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    T value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    if constexpr (TEnumTraits<T>::IsStringSerializableEnum) {
        ToUnversionedValue(unversionedValue, NYT::FormatEnum(value), rowBuffer, id, flags);
    } else if constexpr (TEnumTraits<T>::IsBitEnum) {
        ToUnversionedValue(unversionedValue, static_cast<ui64>(value), rowBuffer, id, flags);
    } else {
        ToUnversionedValue(unversionedValue, static_cast<i64>(value), rowBuffer, id, flags);
    }
}

template <class T>
    requires TEnumTraits<T>::IsEnum
void FromUnversionedValue(
    T* value,
    TUnversionedValue unversionedValue)
{
    switch (unversionedValue.Type) {
        case EValueType::Int64:
            *value = static_cast<T>(unversionedValue.Data.Int64);
            break;
        case EValueType::Uint64:
            *value = static_cast<T>(unversionedValue.Data.Uint64);
            break;
        case EValueType::String:
            *value = NYT::ParseEnum<T>(unversionedValue.AsStringBuf());
            break;
        default:
            THROW_ERROR_EXCEPTION("Cannot parse enum value from %Qlv",
                unversionedValue.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProtobufToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const google::protobuf::Message& value,
    const NYson::TProtobufMessageType* type,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags);

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const T& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
    requires std::is_convertible<T*, google::protobuf::Message*>::value
{
    ProtobufToUnversionedValueImpl(
        unversionedValue,
        value,
        NYson::ReflectProtobufMessageType<T>(),
        rowBuffer,
        id,
        flags);
}

////////////////////////////////////////////////////////////////////////////////

void UnversionedValueToProtobufImpl(
    google::protobuf::Message* value,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    T* value,
    TUnversionedValue unversionedValue)
    requires std::is_convertible<T*, google::protobuf::Message*>::value
{
    UnversionedValueToProtobufImpl(
        value,
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const std::optional<T>& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    if (value) {
        ToUnversionedValue(unversionedValue, *value, rowBuffer, id, flags);
    } else {
        *unversionedValue = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
    }
}

template <class T>
void FromUnversionedValue(
    std::optional<T>* value,
    TUnversionedValue unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        *value = std::nullopt;
    } else {
        value->emplace();
        FromUnversionedValue(&**value, unversionedValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTag>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const TStrongTypedef<T, TTag>& value,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    ToUnversionedValue(unversionedValue, value.Underlying(), rowBuffer, id, flags);
}

template <class T, class TTag>
void FromUnversionedValue(
    TStrongTypedef<T, TTag>* value,
    TUnversionedValue unversionedValue)
{
    FromUnversionedValue(&value->Underlying(), unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToVersionedValue(
    TVersionedValue* versionedValue,
    T&& value,
    const TRowBufferPtr& rowBuffer,
    NTransactionClient::TTimestamp timestamp,
    int id,
    EValueFlags flags)
{
    ToUnversionedValue(
        static_cast<TUnversionedValue*>(versionedValue),
        std::forward<T>(value),
        rowBuffer,
        id,
        flags);
    versionedValue->Timestamp = timestamp;
}

template <class T>
TVersionedValue ToVersionedValue(
    T&& value,
    const TRowBufferPtr& rowBuffer,
    NTransactionClient::TTimestamp timestamp,
    int id,
    EValueFlags flags)
{
    TVersionedValue versionedValue;
    ToVersionedValue(
        &versionedValue,
        std::forward<T>(value),
        rowBuffer,
        timestamp,
        id,
        flags);
    return versionedValue;
}

////////////////////////////////////////////////////////////////////////////////

void ListToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags);

template <class T>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const std::vector<T>& values,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    size_t index = 0;
    ListToUnversionedValueImpl(
        unversionedValue,
        [&] (TUnversionedValue* itemValue) mutable -> bool {
            if (index == values.size()) {
                return false;
            }
            ToUnversionedValue(itemValue, values[index++], rowBuffer);
            return true;
        },
        rowBuffer,
        id,
        flags);
}

void UnversionedValueToListImpl(
    std::function<google::protobuf::Message*()> appender,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    std::vector<T>* values,
    TUnversionedValue unversionedValue)
    requires std::is_convertible<T*, google::protobuf::Message*>::value
{
    values->clear();
    UnversionedValueToListImpl(
        [&] {
            values->emplace_back();
            return &values->back();
        },
        NYson::ReflectProtobufMessageType<T>(),
        unversionedValue);
}

void UnversionedValueToListImpl(
    std::function<void(TUnversionedValue)> appender,
    TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    std::vector<T>* values,
    TUnversionedValue unversionedValue)
    requires TUnversionedValueConversionTraits<T>::Scalar
{
    values->clear();
    UnversionedValueToListImpl(
        [&] (TUnversionedValue itemValue) {
            values->emplace_back();
            FromUnversionedValue(&values->back(), itemValue);
        },
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

void MapToUnversionedValueImpl(
    TUnversionedValue* unversionedValue,
    const std::function<bool(TString*, TUnversionedValue*)> producer,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags);

template <class TKey, class TValue>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const THashMap<TKey, TValue>& map,
    const TRowBufferPtr& rowBuffer,
    int id,
    EValueFlags flags)
{
    auto it = map.begin();
    MapToUnversionedValueImpl(
        unversionedValue,
        [&] (TString* itemKey, TUnversionedValue* itemValue) mutable -> bool {
            if (it == map.end()) {
                return false;
            }
            *itemKey = ToString(it->first);
            ToUnversionedValue(itemValue, it->second, rowBuffer);
            ++it;
            return true;
        },
        rowBuffer,
        id,
        flags);
}

void UnversionedValueToMapImpl(
    std::function<google::protobuf::Message*(TString)> appender,
    const NYson::TProtobufMessageType* type,
    TUnversionedValue unversionedValue);

template <class TKey, class TValue>
void FromUnversionedValue(
    THashMap<TKey, TValue>* map,
    TUnversionedValue unversionedValue)
    requires std::is_convertible<TValue*, ::google::protobuf::Message*>::value
{
    map->clear();
    UnversionedValueToMapImpl(
        [&] (TString key) {
            auto pair = map->emplace(FromString<TKey>(std::move(key)), TValue());
            return &pair.first->second;
        },
        NYson::ReflectProtobufMessageType<TValue>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
auto ToUnversionedValues(
    const TRowBufferPtr& rowBuffer,
    Ts&&... values)
    -> std::array<TUnversionedValue, sizeof...(Ts)>
{
    TRowValueTypesChecker<Ts...>();
    std::array<TUnversionedValue, sizeof...(Ts)> array;
    auto* current = array.data();
    int id = 0;
    (ToUnversionedValue(current++, std::forward<Ts>(values), rowBuffer, id++), ...);
    return array;
}

template <class... Ts>
void FromUnversionedRow(
    TUnversionedRow row,
    Ts*... values)
{
    if (row.GetCount() < sizeof...(Ts)) {
        THROW_ERROR_EXCEPTION("Invalid number of values in row: expected >=%v, got %v",
            sizeof...(Ts),
            row.GetCount());
    }
    const auto* current = row.Begin();
    (FromUnversionedValue(values, *current++), ...);
}

namespace NDetail {

template <size_t Index, class... Ts>
void TupleFromUnversionedRowHelper(std::tuple<Ts...>* tuple, TUnversionedRow row)
{
    if constexpr(Index < sizeof...(Ts)) {
        FromUnversionedValue(&std::get<Index>(*tuple), row[Index]);
        TupleFromUnversionedRowHelper<Index + 1, Ts...>(tuple, row);
    }
}

} // namespace NDetail

template <class... Ts>
std::tuple<Ts...> FromUnversionedRow(TUnversionedRow row)
{
    if  (row.GetCount() < sizeof...(Ts)) {
        THROW_ERROR_EXCEPTION("Invalid number of values in row: expected >=%v, got %v",
            sizeof...(Ts),
            row.GetCount());
    }
    std::tuple<Ts...> result;
    NDetail::TupleFromUnversionedRowHelper<0, Ts...>(&result, row);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TUnversionedValue ToUnversionedValue(T&& value, const TRowBufferPtr& rowBuffer, int id, EValueFlags flags)
{
    TUnversionedValue unversionedValue;
    ToUnversionedValue(&unversionedValue, std::forward<T>(value), rowBuffer, id, flags);
    return unversionedValue;
}

template <class T>
T FromUnversionedValue(TUnversionedValue unversionedValue)
{
    T value;
    FromUnversionedValue(&value, unversionedValue);
    return value;
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
TUnversionedOwningRow MakeUnversionedOwningRow(Ts&&... values)
{
    TRowValueTypesChecker<Ts...>();
    constexpr bool AllTypesInline = (... && TUnversionedValueConversionTraits<Ts>::Inline);
    auto rowBuffer = AllTypesInline ? TRowBufferPtr() : New<TRowBuffer>();
    TUnversionedOwningRowBuilder builder(sizeof...(Ts));
    int id = 0;
    (builder.AddValue(ToUnversionedValue(std::forward<Ts>(values), rowBuffer, id++)), ...);
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
void TUnversionedRowsBuilder::AddRow(Ts&&... values)
{
    TRowValueTypesChecker<Ts...>();
    auto row = RowBuffer_->AllocateUnversioned(sizeof...(Ts));
    auto* current = row.Begin();
    int id = 0;
    (ToUnversionedValue(current++, std::forward<Ts>(values), RowBuffer_, id++, /*flags*/ EValueFlags::None), ...);
    Rows_.push_back(row);
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class... TArgs>
auto ReadRowBatch(const TIntrusivePtr<TReader>& reader, TArgs&&... args)
{
    while (true) {
        auto batch = reader->Read(std::forward<TArgs>(args)...);
        if (!batch || !batch->IsEmpty()) {
            return batch;
        }
        NConcurrency::WaitFor(reader->GetReadyEvent())
            .ThrowOnError();
    }
}

template <class TWriter, class... TArgs>
auto WriteRowBatch(const TIntrusivePtr<TWriter>& writer, TArgs&&... args)
{
    if (!writer->Write(std::forward<TArgs>(args)...)) {
        NConcurrency::WaitFor(writer->GetReadyEvent())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
