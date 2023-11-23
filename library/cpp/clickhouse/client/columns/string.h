#pragma once

#include "column.h"

#include <util/generic/string.h>

namespace NClickHouse {
    /**
 * Represents column of fixed-length strings.
 */
    class TColumnFixedString: public TColumn {
    public:
        static TIntrusivePtr<TColumnFixedString> Create(size_t n);
        static TIntrusivePtr<TColumnFixedString> Create(size_t n, const TVector<TString>& data);

        /// Appends one element to the column.
        void Append(const TString& str);

        /// Returns element at given row number.
        const TString& At(size_t n) const;

        /// Returns element at given row number.
        const TString& operator[](size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const TString& value);

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
        TColumnFixedString(size_t n);
        TColumnFixedString(size_t n, const TVector<TString>& data);

        const size_t StringSize_;
        TVector<TString> Data_;
    };

    /**
 * Represents column of variable-length strings.
 */
    class TColumnString: public TColumn {
    public:
        static TIntrusivePtr<TColumnString> Create();
        static TIntrusivePtr<TColumnString> Create(const TVector<TString>& data);
        static TIntrusivePtr<TColumnString> Create(TVector<TString>&& data);

        /// Appends one element to the column.
        void Append(const TString& str);

        /// Returns element at given row number.
        const TString& At(size_t n) const;

        /// Returns element at given row number.
        const TString& operator[](size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, const TString& value);

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
        TColumnString();
        TColumnString(const TVector<TString>& data);
        TColumnString(TVector<TString>&& data);

        TVector<TString> Data_;
    };

    /**
* Represents column of variable-length strings but use TStringBuf instead TString.
*/
    class TColumnStringBuf: public NClickHouse::TColumn {
    public:
        static TIntrusivePtr<TColumnStringBuf> Create();
        static TIntrusivePtr<TColumnStringBuf> Create(const TVector<TStringBuf>& data);
        static TIntrusivePtr<TColumnStringBuf> Create(TVector<TStringBuf>&& data);

        /// Appends one element to the column.
        void Append(TStringBuf str);

        /// Returns element at given row number.
        const TStringBuf& At(size_t n) const;

        /// Returns element at given row number.
        const TStringBuf& operator[](size_t n) const;

        /// Set element at given row number.
        void SetAt(size_t n, TStringBuf value);

    public:
        /// Appends content of given column to the end of current one.
        void Append(NClickHouse::TColumnRef column) override;

        /// Loads column data from input stream.
        bool Load(NClickHouse::TCodedInputStream* input, size_t rows) override;

        /// Saves column data to output stream.
        void Save(NClickHouse::TCodedOutputStream* output) override;

        /// Returns count of rows in the column.
        size_t Size() const override;

        /// Makes slice of the current column.
        NClickHouse::TColumnRef Slice(size_t begin, size_t len) override;

    private:
        TColumnStringBuf();
        TColumnStringBuf(const TVector<TStringBuf>& data);
        TColumnStringBuf(TVector<TStringBuf>&& data);

        TVector<TStringBuf> Data_;
    };

}
