#pragma once

#include "column.h"

namespace NClickHouse {
    /**
 * Represents various numeric columns.
 */
    template <typename T>
    class TColumnVector: public TColumn {
    public:
        static TIntrusivePtr<TColumnVector<T>> Create();
        static TIntrusivePtr<TColumnVector<T>> Create(const TVector<T>& data);
        static TIntrusivePtr<TColumnVector<T>> Create(TVector<T>&& data);

        /// Appends one element to the end of column.
        void Append(const T& value);

        /// Returns element at given row number.
        const T& At(size_t n) const;

        /// Returns element at given row number.
        const T& operator[](size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const T& value);

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
        TColumnVector();
        TColumnVector(const TVector<T>& data);
        TColumnVector(TVector<T>&& data);

        TVector<T> Data_;
    };

    using TColumnUInt8 = TColumnVector<ui8>;
    using TColumnUInt16 = TColumnVector<ui16>;
    using TColumnUInt32 = TColumnVector<ui32>;
    using TColumnUInt64 = TColumnVector<ui64>;

    using TColumnInt8 = TColumnVector<i8>;
    using TColumnInt16 = TColumnVector<i16>;
    using TColumnInt32 = TColumnVector<i32>;
    using TColumnInt64 = TColumnVector<i64>;

    using TColumnFloat32 = TColumnVector<float>;
    using TColumnFloat64 = TColumnVector<double>;

}
