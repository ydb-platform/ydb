#pragma once

#include <library/cpp/clickhouse/client/base/coded.h>
#include <library/cpp/clickhouse/client/types/types.h>

#include <util/generic/ptr.h>

namespace NClickHouse {
    using TColumnRef = TIntrusivePtr<class TColumn>;

    /**
 * An abstract base of all columns classes.
 */
    class TColumn: public TAtomicRefCount<TColumn> {
    public:
        virtual ~TColumn() {
        }

        /// Downcast pointer to the specific culumn's subtype.
        template <typename T>
        inline TIntrusivePtr<T> As() {
            return TIntrusivePtr<T>(dynamic_cast<T*>(this));
        }

        /// Downcast pointer to the specific culumn's subtype.
        template <typename T>
        inline TIntrusivePtr<const T> As() const {
            return TIntrusivePtr<const T>(dynamic_cast<const T*>(this));
        }

        /// Get type object of the column.
        inline TTypeRef Type() const {
            return Type_;
        }

        /// Appends content of given column to the end of current one.
        virtual void Append(TColumnRef column) = 0;

        /// Loads column data from input stream.
        virtual bool Load(TCodedInputStream* input, size_t rows) = 0;

        /// Saves column data to output stream.
        virtual void Save(TCodedOutputStream* output) = 0;

        /// Returns count of rows in the column.
        virtual size_t Size() const = 0;

        /// Makes slice of the current column.
        virtual TColumnRef Slice(size_t begin, size_t len) = 0;

    protected:
        explicit inline TColumn(TTypeRef type)
            : Type_(type)
        {
        }

        TTypeRef Type_;
    };

}
