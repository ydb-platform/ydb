#pragma once

#include "columns/column.h"

namespace NClickHouse {
    struct TBlockInfo {
        ui8 IsOverflows = 0;
        i32 BucketNum = -1;
    };

    class TBlock {
    public:
        /// Allow to iterate over block's columns.
        class TIterator {
        public:
            TIterator(const TBlock& block);

            /// Name of column.
            const TString& Name() const;

            /// Type of column.
            TTypeRef Type() const;

            /// Reference to column object.
            TColumnRef Column() const;

            /// Move to next column.
            void Next();

            /// Is the iterator still valid.
            bool IsValid() const;

        private:
            TIterator() = delete;

            const TBlock& Block_;
            size_t Idx_;
        };

    public:
        TBlock();
        TBlock(size_t cols, size_t rows);
        ~TBlock();

        /// Append named column to the block.
        void AppendColumn(const TString& name, const TColumnRef& col);

        /// Count of columns in the block.
        size_t GetColumnCount() const;

        const TBlockInfo& Info() const;

        /// Count of rows in the block.
        size_t GetRowCount() const;

        /// Append block to the current (vertical scale)
        void AppendBlock(const TBlock& block);

        /// Reference to column by index in the block.
        TColumnRef operator[](size_t idx) const;

    private:
        struct TColumnItem {
            TString Name;
            TColumnRef Column;
        };

        TBlockInfo Info_;
        TVector<TColumnItem> Columns_;
        /// Count of rows in the block.
        size_t Rows_;
    };

}
