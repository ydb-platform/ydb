#include "enum.h"
#include "utils.h"
#include <util/string/printf.h>

namespace NClickHouse {
    template <typename T>
    TColumnEnum<T>::TColumnEnum(TTypeRef type)
        : TColumn(type)
    {
    }

    template <typename T>
    TColumnEnum<T>::TColumnEnum(TTypeRef type, const TVector<T>& data)
        : TColumn(type)
        , Data_(data)
    {
    }

    template <>
    TIntrusivePtr<TColumnEnum<i8>> TColumnEnum<i8>::Create(const TVector<TEnumItem>& enumItems) {
        TTypeRef type = TType::CreateEnum8(enumItems);
        return new TColumnEnum<i8>(type);
    }

    template <>
    TIntrusivePtr<TColumnEnum<i8>> TColumnEnum<i8>::Create(
        const TVector<TEnumItem>& enumItems,
        const TVector<i8>& values,
        bool checkValues) {
        TTypeRef type = TType::CreateEnum8(enumItems);
        if (checkValues) {
            for (i8 value : values) {
                Y_ENSURE(type->HasEnumValue(value), Sprintf("Enum type doesn't have value %d", value));
            }
        }
        return new TColumnEnum<i8>(type, values);
    }

    template <>
    TIntrusivePtr<TColumnEnum<i8>> TColumnEnum<i8>::Create(
        const TVector<TEnumItem>& enumItems,
        const TVector<TString>& names) {
        TTypeRef type = TType::CreateEnum8(enumItems);
        TVector<i8> values;
        values.reserve(names.size());
        for (const TString& name : names) {
            values.push_back(type->GetEnumValue(name));
        }
        return new TColumnEnum<i8>(type, values);
    }

    template <>
    TIntrusivePtr<TColumnEnum<i16>> TColumnEnum<i16>::Create(const TVector<TEnumItem>& enumItems) {
        TTypeRef type = TType::CreateEnum16(enumItems);
        return new TColumnEnum<i16>(type);
    }

    template <>
    TIntrusivePtr<TColumnEnum<i16>> TColumnEnum<i16>::Create(
        const TVector<TEnumItem>& enumItems,
        const TVector<i16>& values,
        bool checkValues) {
        TTypeRef type = TType::CreateEnum16(enumItems);
        if (checkValues) {
            for (i16 value : values) {
                Y_ENSURE(type->HasEnumValue(value), Sprintf("Enum type doesn't have value %d", value));
            }
        }
        return new TColumnEnum<i16>(type, values);
    }

    template <>
    TIntrusivePtr<TColumnEnum<i16>> TColumnEnum<i16>::Create(
        const TVector<TEnumItem>& enumItems,
        const TVector<TString>& names) {
        TTypeRef type = TType::CreateEnum16(enumItems);
        TVector<i16> values;
        values.reserve(names.size());
        for (const TString& name : names) {
            values.push_back(type->GetEnumValue(name));
        }
        return new TColumnEnum<i16>(type, values);
    }

    template <typename T>
    void TColumnEnum<T>::Append(const T& value, bool checkValue) {
        if (checkValue) {
            Y_ENSURE(Type_->HasEnumValue(value), Sprintf("Enum type doesn't have value %d", value));
        }
        Data_.push_back(value);
    }

    template <typename T>
    void TColumnEnum<T>::Append(const TString& name) {
        Data_.push_back(Type_->GetEnumValue(name));
    }

    template <typename T>
    const T& TColumnEnum<T>::At(size_t n) const {
        return Data_.at(n);
    }

    template <typename T>
    const TString& TColumnEnum<T>::NameAt(size_t n) const {
        return Type_->GetEnumName(Data_.at(n));
    }

    template <typename T>
    const T& TColumnEnum<T>::operator[](size_t n) const {
        return Data_[n];
    }

    template <typename T>
    void TColumnEnum<T>::SetAt(size_t n, const T& value, bool checkValue) {
        if (checkValue) {
            Y_ENSURE(Type_->HasEnumValue(value), Sprintf("Enum type doesn't have value %d", value));
        }
        Data_.at(n) = value;
    }

    template <typename T>
    void TColumnEnum<T>::SetNameAt(size_t n, const TString& name) {
        Data_.at(n) = Type_->GetEnumValue(name);
    }

    template <typename T>
    void TColumnEnum<T>::Append(TColumnRef column) {
        if (auto col = column->As<TColumnEnum<T>>()) {
            Data_.insert(Data_.end(), col->Data_.begin(), col->Data_.end());
        }
    }

    template <typename T>
    bool TColumnEnum<T>::Load(TCodedInputStream* input, size_t rows) {
        Data_.resize(rows);
        return input->ReadRaw(Data_.data(), Data_.size() * sizeof(T));
    }

    template <typename T>
    void TColumnEnum<T>::Save(TCodedOutputStream* output) {
        output->WriteRaw(Data_.data(), Data_.size() * sizeof(T));
    }

    template <typename T>
    size_t TColumnEnum<T>::Size() const {
        return Data_.size();
    }

    template <typename T>
    TColumnRef TColumnEnum<T>::Slice(size_t begin, size_t len) {
        return new TColumnEnum<T>(Type_, SliceVector(Data_, begin, len));
    }

    template class TColumnEnum<i8>;
    template class TColumnEnum<i16>;

}
