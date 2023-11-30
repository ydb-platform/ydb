#pragma once

#include "numeric.h"

namespace NClickHouse {
    /**
 * Represents column of Array(T).
 */
    class TColumnArray: public TColumn {
    public:
        static TIntrusivePtr<TColumnArray> Create(TColumnRef data);

        static TIntrusivePtr<TColumnArray> Create(TColumnRef data, TVector<ui64>&& offsets);

        /// Converts input column to array and appends
        /// as one row to the current column.
        void AppendAsColumn(TColumnRef array);

        /// Convets array at pos n to column.
        /// Type of element of result column same as type of array element.
        TColumnRef GetAsColumn(size_t n) const;

    public:
        /// Appends content of given column to the end of current one.
        void Append(TColumnRef) override;

        /// Loads column data from input stream.
        bool Load(TCodedInputStream* input, size_t rows) override;

        /// Saves column data to output stream.
        void Save(TCodedOutputStream* output) override;

        /// Returns count of rows in the column.
        size_t Size() const override;

        /// Makes slice of the current column.
        TColumnRef Slice(size_t, size_t) override {
            return TColumnRef();
        }

    private:
        TColumnArray(TColumnRef data);

        TColumnArray(TColumnRef data, TVector<ui64>&& offsets);

        size_t GetOffset(size_t n) const;

        size_t GetSize(size_t n) const;

    private:
        TColumnRef Data_;
        TIntrusivePtr<TColumnUInt64> Offsets_;
    };

}
