#pragma once

#include "data.h"
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/vector.h>

namespace NAnalytics {

// Get rid of NaNs and INFs
inline double Finitize(double x, double notFiniteValue = 0.0)
{
    return isfinite(x)? x: notFiniteValue;
}

inline void ParseNameAndOpts(const TString& nameAndOpts, TString& name, THashSet<TString>& opts)
{
    name.clear();
    opts.clear();
    bool first = true;
    auto vs = SplitString(nameAndOpts, "-");
    for (const auto& s : vs) {
        if (first) {
            name = s;
            first = false;
        } else {
            opts.insert(s);
        }
    }
}

inline TString ParseName(const TString& nameAndOpts)
{
    auto vs = SplitString(nameAndOpts, "-");
    if (vs.empty()) {
        return TString();
    } else {
        return vs[0];
    }
}

template <class R, class T>
inline R AccumulateIfExist(const TString& name, const TTable& table, R r, T t)
{
    ForEach(table.begin(), table.end(), [=,&r] (const TRow& row) {
        double value;
        if (row.Get(name, value)) {
            r = t(r, value);
        }
    });
    return r;
}

inline double MinValue(const TString& nameAndOpts, const TTable& table)
{
    TString name;
    THashSet<TString> opts;
    ParseNameAndOpts(nameAndOpts, name, opts);
    bool stack = opts.contains("stack");
    if (stack) {
        return 0.0;
    } else {
        auto zero = 0.0;

        return AccumulateIfExist(name, table, 1.0 / zero /*+inf*/, [] (double x, double y) {
            return Min(x, y);
        });
    }
}

inline double MaxValue(const TString& nameAndOpts, const TTable& table)
{
    TString name;
    THashSet<TString> opts;
    ParseNameAndOpts(nameAndOpts, name, opts);
    bool stack = opts.contains("stack");
    if (stack) {
        return AccumulateIfExist(name, table, 0.0, [] (double x, double y) {
            return x + y;
        });
    } else {
        auto zero = 0.0;

        return AccumulateIfExist(name, table, -1.0 / zero /*-inf*/, [] (double x, double y) {
            return Max(x, y);
        });
    }
}

template <class T>
inline void Map(TTable& table, const TString& rname, T t)
{
    ForEach(table.begin(), table.end(), [=] (TRow& row) {
        row[rname] = t(row);
    });
}

inline std::function<bool(const TRow&)> HasNoValueFor(TString name)
{
    return [=] (const TRow& row) -> bool {
        double value;
        return !row.Get(name, value);
    };
}


inline std::function<double(const TRow&)> GetValueFor(TString name, double defVal = 0.0)
{
    return [=] (const TRow& row) -> double {
        double value;
        return row.Get(name, value)? value: defVal;
    };
}

inline std::function<double(const TRow&)> Const(double defVal = 0.0)
{
    return [=] (const TRow&) {
        return defVal;
    };
}

}
