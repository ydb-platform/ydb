#pragma once

#include "data.h"

namespace NAnalytics {

template <class TSkip, class TX, class TY>
inline TTable Histogram(const TTable& in, TSkip skip,
                 const TString& xn_out, TX x_in,
                 const TString& yn_out, TY y_in,
                 double x1, double x2, double dx)
{
    long buckets = (x2 - x1) / dx;
    TTable out;
    TString yn_sum = yn_out + "_sum";
    TString yn_share = yn_out + "_share";
    double ysum = 0.0;
    out.resize(buckets);
    for (size_t i = 0; i < out.size(); i++) {
        double lb = x1 + dx*i;
        double ub = lb + dx;
        out[i].Name = "[" + ToString(lb) + ";" + ToString(ub) + (ub==x2? "]": ")");
        out[i][xn_out] = (lb + ub) / 2;
        out[i][yn_sum] = 0.0;
    }
    for (const auto& row : in) {
        if (skip(row)) {
            continue;
        }
        double x = x_in(row);
        long i = (x - x1) / dx;
        if (x == x2) { // Special hack to include right edge
            i--;
        }
        double y = y_in(row);
        ysum += y;
        if (i >= 0 && i < buckets) {
            out[i][yn_sum] = y + out[i].GetOrDefault(yn_sum, 0.0);
        }
    }
    for (TRow& row : out) {
        if (ysum != 0.0) {
            row[yn_share] = row.GetOrDefault(yn_sum, 0.0) / ysum;
        }
    }
    return out;
}

inline TTable HistogramAll(const TTable& in, const TString& xn, double x1, double x2, double dx)
{
    long buckets = (dx == 0.0? 1: (x2 - x1) / dx);
    TTable out;
    THashMap<TString, double> colSum;
    out.resize(buckets);

    TSet<TString> cols;
    for (auto& row : in) {
        for (auto& kv : row) {
            cols.insert(kv.first);
        }
    }
    cols.insert("_count");
    cols.erase(xn);

    for (const TString& col : cols) {
        colSum[col] = 0.0;
    }

    for (size_t i = 0; i < out.size(); i++) {
        double lb = x1 + dx*i;
        double ub = lb + dx;
        TRow& row = out[i];
        row.Name = "[" + ToString(lb) + ";" + ToString(ub) + (ub==x2? "]": ")");
        row[xn] = (lb + ub) / 2;
        for (const TString& col : cols) {
            row[col + "_sum"] = 0.0;
        }
    }
    for (const TRow& row_in : in) {
        double x;
        if (!row_in.Get(xn, x)) {
            continue;
        }
        long i = (dx == 0.0? 0: (x - x1) / dx);
        if (x == x2 && dx > 0.0) { // Special hack to include right edge
            i--;
        }
        for (const auto& kv : row_in) {
            const TString& yn = kv.first;
            if (yn == xn) {
                continue;
            }
            double y;
            if (!row_in.Get(yn, y)) {
                continue;
            }
            colSum[yn] += y;
            if (i >= 0 && i < buckets) {
                out[i][yn + "_cnt"] = out[i].GetOrDefault(yn + "_cnt") + 1;
                out[i][yn + "_sum"] = out[i].GetOrDefault(yn + "_sum") + y;
                if (out[i].contains(yn + "_min")) {
                    out[i][yn + "_min"] = Min(y, out[i].GetOrDefault(yn + "_min"));
                } else {
                    out[i][yn + "_min"] = y;
                }
                if (out[i].contains(yn + "_max")) {
                    out[i][yn + "_max"] = Max(y, out[i].GetOrDefault(yn + "_max"));
                } else {
                    out[i][yn + "_max"] = y;
                }
            }
        }
        colSum["_count"]++;
        if (i >= 0 && i < buckets) {
            out[i]["_count_sum"] = out[i].GetOrDefault("_count_sum") + 1;
        }
    }
    for (TRow& row : out) {
        for (const TString& col : cols) {
            double ysum = colSum[col];
            if (col != "_count") {
                if (row.GetOrDefault(col + "_cnt") != 0.0) {
                    row[col + "_avg"] = row.GetOrDefault(col + "_sum") / row.GetOrDefault(col + "_cnt");
                }
            }
            if (ysum != 0.0) {
                row[col + "_share"] = row.GetOrDefault(col + "_sum") / ysum;
            }
        }
    }
    return out;
}

inline TMatrix CovarianceMatrix(const TTable& in)
{
    TSet<TString> cols;
    for (auto& row : in) {
        for (auto& kv : row) {
            cols.insert(kv.first);
        }
    }

    struct TAggregate {
        size_t Idx = 0;
        double Sum = 0;
        size_t Count = 0;
        double Mean = 0;
    };

    THashMap<TString, TAggregate> colAggr;

    size_t colCount = 0;
    for (const TString& col : cols) {
        TAggregate& aggr = colAggr[col];
        aggr.Idx = colCount++;
    }

    for (const TRow& row : in) {
        for (const auto& kv : row) {
            const TString& xn = kv.first;
            double x;
            if (!row.Get(xn, x)) {
                continue;
            }
            TAggregate& aggr = colAggr[xn];
            aggr.Sum += x;
            aggr.Count++;
        }
    }

    for (auto& kv : colAggr) {
        TAggregate& aggr = kv.second;
        aggr.Mean = aggr.Sum / aggr.Count;
    }

    TMatrix covCount(cols.size(), cols.size());
    TMatrix cov(cols.size(), cols.size());
    for (const TRow& row : in) {
        for (const auto& kv1 : row) {
            double x;
            if (!row.Get(kv1.first, x)) {
                continue;
            }
            TAggregate& xaggr = colAggr[kv1.first];
            for (const auto& kv2 : row) {
                double y;
                if (!row.Get(kv2.first, y)) {
                    continue;
                }                
                TAggregate& yaggr = colAggr[kv2.first];
                covCount.Cell(xaggr.Idx, yaggr.Idx)++;
                cov.Cell(xaggr.Idx, yaggr.Idx) += (x - xaggr.Mean) * (y - yaggr.Mean);
            }
        }
    }

    for (size_t idx = 0; idx < cov.size(); idx++) {
        cov[idx] /= covCount[idx];
    }

    return cov;
}

}
