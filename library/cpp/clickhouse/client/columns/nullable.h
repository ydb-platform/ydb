#pragma once

#include "column.h"
#include "numeric.h"

namespace NClickHouse {
    /**
 * Represents column of Nullable(T).
 */
    class TColumnNullable: public TColumn {
    public:
        static TIntrusivePtr<TColumnNullable> Create(TColumnRef nested);
        static TIntrusivePtr<TColumnNullable> Create(TColumnRef nested, TColumnRef nulls);

        /// Returns null flag at given row number.
        bool IsNull(size_t n) const;

        /// Returns nested column.
        TColumnRef Nested() const;

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
        TColumnNullable(TColumnRef nested, TColumnRef nulls);

        TColumnRef Nested_;
        TIntrusivePtr<TColumnUInt8> Nulls_;
    };

}
