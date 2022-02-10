#pragma once

#include <ydb/library/binary_json/read.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/maybe.h>

#include <variant>

namespace NYql::NJsonPath {

enum class EValueType {
    Bool = 0,
    Number = 1,
    String = 2,
    Null = 4,
    Object = 5,
    Array = 6,
};

struct TEmptyMarker {
};

class TValue;

class TArrayIterator {
public:
    TArrayIterator();
    explicit TArrayIterator(const NUdf::TUnboxedValue& iterator);
    explicit TArrayIterator(NUdf::TUnboxedValue&& iterator);

    explicit TArrayIterator(const NKikimr::NBinaryJson::TArrayIterator& iterator);
    explicit TArrayIterator(NKikimr::NBinaryJson::TArrayIterator&& iterator);

    bool Next(TValue& value);

private:
    std::variant<TEmptyMarker, NUdf::TUnboxedValue, NKikimr::NBinaryJson::TArrayIterator> Iterator;
};

class TObjectIterator {
public:
    TObjectIterator();
    explicit TObjectIterator(const NUdf::TUnboxedValue& iterator);
    explicit TObjectIterator(NUdf::TUnboxedValue&& iterator);

    explicit TObjectIterator(const NKikimr::NBinaryJson::TObjectIterator& iterator);
    explicit TObjectIterator(NKikimr::NBinaryJson::TObjectIterator&& iterator);

    bool Next(TValue& key, TValue& value);

private:
    std::variant<TEmptyMarker, NUdf::TUnboxedValue, NKikimr::NBinaryJson::TObjectIterator> Iterator;
};

class TValue {
public:
    TValue();
    explicit TValue(const NUdf::TUnboxedValue& value);
    explicit TValue(NUdf::TUnboxedValue&& value);

    explicit TValue(const NKikimr::NBinaryJson::TEntryCursor& value);
    explicit TValue(NKikimr::NBinaryJson::TEntryCursor&& value);

    explicit TValue(const NKikimr::NBinaryJson::TContainerCursor& value);
    explicit TValue(NKikimr::NBinaryJson::TContainerCursor&& value);

    EValueType GetType() const;
    bool Is(EValueType type) const;
    bool IsBool() const;
    bool IsNumber() const;
    bool IsString() const;
    bool IsNull() const;
    bool IsObject() const;
    bool IsArray() const;

    // Scalar value methods
    double GetNumber() const;
    bool GetBool() const;
    const TStringBuf GetString() const;

    ui32 GetSize() const;

    // Array methods
    TValue GetElement(ui32 index) const;
    TArrayIterator GetArrayIterator() const;

    // Object methods
    TMaybe<TValue> Lookup(const TStringBuf key) const;
    TObjectIterator GetObjectIterator() const;

    NUdf::TUnboxedValue ConvertToUnboxedValue(const NUdf::IValueBuilder* valueBuilder) const;

private:
    void UnpackInnerValue();

    std::variant<NUdf::TUnboxedValue, NKikimr::NBinaryJson::TEntryCursor, NKikimr::NBinaryJson::TContainerCursor> Value;
};

}
