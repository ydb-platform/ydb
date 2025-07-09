#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NAnalytics {

struct TRow : public THashMap<TString, double> {
    TString Name;

    bool Get(const TString& name, double& value) const
    {
        if (name == "_count") { // Special values
            value = 1.0;
            return true;
        }
        auto iter = find(name);
        if (iter != end()) {
            value = iter->second;
            return true;
        } else {
            return false;
        }
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
