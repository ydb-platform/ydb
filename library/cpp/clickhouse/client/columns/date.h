#pragma once

#include "numeric.h"

#include <util/datetime/base.h>

namespace NClickHouse {
    /** */
    class TColumnDate: public TColumn {
    public:
        static TIntrusivePtr<TColumnDate> Create();
        static TIntrusivePtr<TColumnDate> Create(const TVector<TInstant>& data);

        /// Appends one element to the end of column.
        void Append(const TInstant& value);

        /// Returns element at given row number.
        std::time_t At(size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const TInstant& value);

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
        TColumnDate();
        TColumnDate(const TVector<TInstant>& data);

        TIntrusivePtr<TColumnUInt16> Data_;
    };

    /** */
    class TColumnDateTime: public TColumn {
    public:
        static TIntrusivePtr<TColumnDateTime> Create();
        static TIntrusivePtr<TColumnDateTime> Create(const TVector<TInstant>& data);

        /// Appends one element to the end of column.
        void Append(const TInstant& value);

        /// Returns element at given row number.
        std::time_t At(size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const TInstant& value);

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
        TColumnDateTime();
        TColumnDateTime(const TVector<TInstant>& data);

        TIntrusivePtr<TColumnUInt32> Data_;
    };

}
