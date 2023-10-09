#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <variant>

namespace NAnalytics {

using TRowValue = std::variant<i64, ui64, double, TString>;

TString ToString(const TRowValue& val) {
    TStringBuilder builder;
    std::visit([&builder] (auto&& arg) {
        builder << arg;
    }, val);
    return builder;
}

struct TRow : public THashMap<TString, TRowValue> {
    TString Name;

    template<typename T>
    bool Get(const TString& name, T& value) const {
        if constexpr (std::is_same_v<double, T>) {
            if (name == "_count") { // Special values
                value = 1.0;
                return true;
            }
        }
        auto iter = find(name);
        if (iter != end()) {
            try {
                value = std::get<T>(iter->second);
                return true;
            } catch (...) {}
        }
        return false;
    }

    template<typename T = double>
    T GetOrDefault(const TString& name, T dflt = T()) {
        Get(name, dflt);
        return dflt;
    }

    bool GetAsString(const TString& name, TString& value) const {
        auto iter = find(name);
        if (iter != end()) {
            value = ToString(iter->second);
            return true;
        }
        return false;
    }
};

using TAttributes = THashMap<TString, TString>;

struct TTable : public TVector<TRow> {
    TAttributes Attributes;
};

struct TMatrix : public TVector<double> {
    size_t Rows;
    size_t Cols;

    explicit TMatrix(size_t rows = 0, size_t cols = 0)
        : TVector<double>(rows * cols)
        , Rows(rows)
        , Cols(cols)
    {}

    void Reset(size_t rows, size_t cols)
    {
        Rows = rows;
        Cols = cols;
        clear();
        resize(rows * cols);
    }

    double& Cell(size_t row, size_t col)
    {
        Y_ABORT_UNLESS(row < Rows);
        Y_ABORT_UNLESS(col < Cols);
        return operator[](row * Cols + col);
    }

    double Cell(size_t row, size_t col) const
    {
        Y_ABORT_UNLESS(row < Rows);
        Y_ABORT_UNLESS(col < Cols);
        return operator[](row * Cols + col);
    }

    double CellSum() const
    {
        double sum = 0.0;
        for (double x : *this) {
            sum += x;
        }
        return sum;
    }
};

}
