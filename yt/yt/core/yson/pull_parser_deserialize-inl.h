#ifndef PULL_PARSER_DESERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include pull_parser_deserialize.h"
// For the sake of sane code completion.
#include "pull_parser_deserialize.h"
#endif

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/token_writer.h>

#include <vector>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

inline void SkipAttributes(TYsonPullParserCursor* cursor)
{
    cursor->SkipAttributes();
    if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
        THROW_ERROR_EXCEPTION("Repeated attributes are not allowed");
    }
}

inline void MaybeSkipAttributes(TYsonPullParserCursor* cursor)
{
    if ((*cursor)->GetType() == EYsonItemType::BeginAttributes) {
        SkipAttributes(cursor);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
void DeserializeVector(T& value, TYsonPullParserCursor* cursor)
{
    int index = 0;
    auto itemVisitor = [&] (TYsonPullParserCursor* cursor) {
        if (index < static_cast<int>(value.size())) {
            Deserialize(value[index], cursor);
        } else {
            value.emplace_back();
            Deserialize(value.back(), cursor);
        }
        ++index;
    };

    if (cursor->TryConsumeFragmentStart()) {
        while ((*cursor)->GetType() != EYsonItemType::EndOfStream) {
            itemVisitor(cursor);
        }
    } else {
        MaybeSkipAttributes(cursor);
        cursor->ParseList(itemVisitor);
    }

    value.resize(index);
}

template <class T>
void DeserializeSet(T& value, TYsonPullParserCursor* cursor)
{
    value.clear();

    auto itemVisitor = [&] (TYsonPullParserCursor* cursor) {
        value.insert(ExtractTo<typename T::value_type>(cursor));
    };

    if (cursor->TryConsumeFragmentStart()) {
        while ((*cursor)->GetType() != EYsonItemType::EndOfStream) {
            itemVisitor(cursor);
        }
    } else {
        MaybeSkipAttributes(cursor);
        cursor->ParseList(itemVisitor);
    }
}

template <class T>
void DeserializeMap(T& value, TYsonPullParserCursor* cursor)
{
    value.clear();

    auto itemVisitor = [&] (TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<typename T::key_type>(cursor);
        auto item = ExtractTo<typename T::mapped_type>(cursor);
        if (value.contains(key)) {
            THROW_ERROR_EXCEPTION("Duplicate key %Qv", key);
        }
        value.emplace(std::move(key), std::move(item));
    };

    if (cursor->TryConsumeFragmentStart()) {
        while ((*cursor)->GetType() != EYsonItemType::EndOfStream) {
            itemVisitor(cursor);
        }
    } else {
        MaybeSkipAttributes(cursor);
        cursor->ParseMap(itemVisitor);
    }
}

template <class T, bool IsSet = std::is_same<typename T::key_type, typename T::value_type>::value>
struct TAssociativeHelper;

template <class T>
struct TAssociativeHelper<T, true>
{
    static void Deserialize(T& value, TYsonPullParserCursor* cursor)
    {
        DeserializeSet(value, cursor);
    }
};

template <class T>
struct TAssociativeHelper<T, false>
{
    static void Deserialize(T& value, TYsonPullParserCursor* cursor)
    {
        DeserializeMap(value, cursor);
    }
};

template <class T>
void DeserializeAssociative(T& value, TYsonPullParserCursor* cursor)
{
    TAssociativeHelper<T>::Deserialize(value, cursor);
}

template <class T, size_t Size = std::tuple_size<T>::value>
struct TTupleHelper;

template <class T>
struct TTupleHelper<T, 0U>
{
    static void DeserializeItem(T&, TYsonPullParserCursor*) {}
};

template <class T, size_t Size>
struct TTupleHelper
{
    static void DeserializeItem(T& value, TYsonPullParserCursor* cursor)
    {
        TTupleHelper<T, Size - 1U>::DeserializeItem(value, cursor);
        if ((*cursor)->GetType() != EYsonItemType::EndList) {
            Deserialize(std::get<Size - 1U>(value), cursor);
        }
    }
};

template <class T>
void DeserializeTuple(T& value, TYsonPullParserCursor* cursor)
{
    const auto isListFragment = cursor->TryConsumeFragmentStart();

    if (!isListFragment) {
        MaybeSkipAttributes(cursor);
        EnsureYsonToken("tuple", *cursor, EYsonItemType::BeginList);
        cursor->Next();
    }

    TTupleHelper<T>::DeserializeItem(value, cursor);

    auto endItemType = isListFragment
        ? EYsonItemType::EndOfStream
        : EYsonItemType::EndList;
    while ((*cursor)->GetType() != endItemType) {
        cursor->SkipComplexValue();
    }

    if (!isListFragment) {
        cursor->Next();
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, class A>
void Deserialize(std::vector<T, A>& value, NYson::TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeVector(value, cursor);
}

template <class T, class A>
void Deserialize(std::deque<T, A>& value, NYson::TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeVector(value, cursor);
}

template <class T>
void Deserialize(std::optional<T>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    MaybeSkipAttributes(cursor);
    if ((*cursor)->GetType() == EYsonItemType::EntityValue) {
        value.reset();
        cursor->Next();
    } else {
        if (!value) {
            value.emplace();
        }
        Deserialize(*value, cursor);
    }
}

// Enum.
template <class T>
requires TEnumTraits<T>::IsEnum
void Deserialize(T& value, TYsonPullParserCursor* cursor)
{
    MaybeSkipAttributes(cursor);
    if constexpr (TEnumTraits<T>::IsBitEnum) {
        switch ((*cursor)->GetType()) {
            case EYsonItemType::BeginList:
                value = T();
                cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
                    EnsureYsonToken("bit enum", *cursor, EYsonItemType::StringValue);
                    value |= ParseEnum<T>((*cursor)->UncheckedAsString());
                    cursor->Next();
                });
                break;
            case EYsonItemType::StringValue:
                value = ParseEnum<T>((*cursor)->UncheckedAsString());
                cursor->Next();
                break;
            default:
                ThrowUnexpectedYsonTokenException(
                    "bit enum",
                    *cursor,
                    {EYsonItemType::BeginList, EYsonItemType::StringValue});
        }
    } else {
        EnsureYsonToken("enum", *cursor, EYsonItemType::StringValue);
        value = ParseEnum<T>((*cursor)->UncheckedAsString());
        cursor->Next();
    }
}

// TCompactVector
template <class T, size_t N>
void Deserialize(TCompactVector<T, N>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeVector(value, cursor);
}

template <class F, class S>
void Deserialize(std::pair<F, S>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<F, S>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

template <class T, size_t N>
void Deserialize(std::array<T, N>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

template <class... T>
void Deserialize(std::tuple<T...>& value, TYsonPullParserCursor* cursor, std::enable_if_t<ArePullParserDeserializable<T...>(), void*>)
{
    NDetail::DeserializeTuple(value, cursor);
}

// For any associative container.
template <template<typename...> class C, class... T, class K>
void Deserialize(
    C<T...>& value,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<typename NDetail::TRemoveConst<typename C<T...>::value_type>::Type>(), void*>)
{
    NDetail::DeserializeAssociative(value, cursor);
}

template <class E, class T, E Min, E Max>
void Deserialize(
    TEnumIndexedArray<E, T, Min, Max>& vector,
    TYsonPullParserCursor* cursor,
    std::enable_if_t<ArePullParserDeserializable<T>(), void*>)
{
    vector = {};

    auto itemVisitor = [&] (TYsonPullParserCursor* cursor) {
        auto key = ExtractTo<E>(cursor);
        if (!vector.IsValidIndex(key)) {
            THROW_ERROR_EXCEPTION("Enum value %Qlv is out of supported range",
                key);
        }
        Deserialize(vector[key], cursor);
    };

    if (cursor->TryConsumeFragmentStart()) {
        while ((*cursor)->GetType() != EYsonItemType::EndOfStream) {
            itemVisitor(cursor);
        }
    } else {
        MaybeSkipAttributes(cursor);
        cursor->ParseMap(itemVisitor);
    }
}

template <typename T>
void DeserializePtr(T& value, TYsonPullParserCursor* cursor)
{
    if ((*cursor)->GetType() != EYsonItemType::BeginAttributes) {
        if ((*cursor)->GetType() == EYsonItemType::EntityValue) {
            cursor->Next();
        } else {
            Deserialize(value, cursor);
        }
        return;
    }

    // We need to place begin attributes token as it
    // will not be recorded.
    TStringStream stream("<");
    {
        cursor->StartRecording(&stream);
        cursor->SkipAttributes();
        if ((*cursor)->GetType() == EYsonItemType::EntityValue) {
            cursor->CancelRecording();
            cursor->Next();
            return;
        }
        cursor->SkipComplexValueAndFinishRecording();
    }
    TYsonPullParser parser(&stream, EYsonType::Node);
    TYsonPullParserCursor newCursor(&parser);
    Deserialize(value, &newCursor);
}

template <class T>
void Deserialize(TIntrusivePtr<T>& value, TYsonPullParserCursor* cursor)
{
    if (!value) {
        value = New<T>();
    }
    DeserializePtr(*value, cursor);
}

template <class T>
void Deserialize(std::unique_ptr<T>& value, TYsonPullParserCursor* cursor)
{
    if (!value) {
        value = std::make_unique<T>();
    }
    DeserializePtr(*value, cursor);
}

template <class T, class TTag>
void Deserialize(TStrongTypedef<T, TTag>& value, TYsonPullParserCursor* cursor)
{
    Deserialize(value.Underlying(), cursor);
}

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
void Deserialize(
    T& message,
    NYson::TYsonPullParserCursor* cursor)
{
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
        NYson::EUnknownYsonFieldsMode::Keep);

    DeserializeProtobufMessage(message, NYson::ReflectProtobufMessageType<T>(), cursor, options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTo>
TTo ExtractTo(TYsonPullParserCursor* cursor)
{
    TTo result;
    Deserialize(result, cursor);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
