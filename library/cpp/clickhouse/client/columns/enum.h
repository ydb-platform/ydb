#pragma once

#include "column.h"

namespace NClickHouse {
    template <typename T>
    class TColumnEnum: public TColumn {
    public:
        static TIntrusivePtr<TColumnEnum<T>> Create(const TVector<TEnumItem>& enumItems);
        static TIntrusivePtr<TColumnEnum<T>> Create(
            const TVector<TEnumItem>& enumItems,
            const TVector<T>& values,
            bool checkValues = false);
        static TIntrusivePtr<TColumnEnum<T>> Create(const TVector<TEnumItem>& enumItems, const TVector<TString>& names);

        /// Appends one element to the end of column.
        void Append(const T& value, bool checkValue = false);
        void Append(const TString& name);

        /// Returns element at given row number.
        const T& At(size_t n) const;
        const TString& NameAt(size_t n) const;

        /// Returns element at given row number.
        const T& operator[](size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const T& value, bool checkValue = false);
        void SetNameAt(size_t n, const TString& name);

    public:
        /// Appends content of given column to the end of current one.
        void Append(TColumnRef column) override;

        /// Loads column data from input stream.
        bool Load(TCodedInputStream* input, size_t rows) override;

        /// Saves column data to output stream.
        void Save(TCodedOutputStream* output) override;

        /// Returns count of rows in the column.
        size_t Size() const override;

        /// Makes slice of the current column.
        TColumnRef Slice(size_t begin, size_t len) override;

    private:
        TColumnEnum(TTypeRef type);
        TColumnEnum(TTypeRef type, const TVector<T>& data);

        TVector<T> Data_;
    };

    using TColumnEnum8 = TColumnEnum<i8>;
    using TColumnEnum16 = TColumnEnum<i16>;

}
