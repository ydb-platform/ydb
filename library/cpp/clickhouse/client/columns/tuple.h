#pragma once

#include "column.h"

#include <util/generic/vector.h>

namespace NClickHouse {
    /** */
    class TColumnTuple: public TColumn {
    public:
        static TIntrusivePtr<TColumnTuple> Create(const TVector<TColumnRef>& columns);

        TColumnRef operator[](size_t n) const {
            return Columns_[n];
        }

        /// Appends content of given column to the end of current one.
        void Append(TColumnRef) override {
        }

        size_t Size() const override;

        bool Load(TCodedInputStream* input, size_t rows) override;

        void Save(TCodedOutputStream* output) override;

        TColumnRef Slice(size_t, size_t) override {
            return TColumnRef();
        }

    private:
        TColumnTuple(const TVector<TColumnRef>& columns);

        TVector<TColumnRef> Columns_;
    };

}
