#pragma once

#include <util/string/printf.h>
#include <util/stream/str.h>
#include <util/generic/set.h>
#include "data.h"

namespace NAnalytics {

inline TString ToCsv(const TTable& in, TString sep = TString("\t"), bool head = true)
{
    TSet<TString> cols;
    bool hasName = false;
    for (const TRow& row : in) {
        hasName = hasName || !row.Name.empty();
        for (const auto& kv : row) {
            cols.insert(kv.first);
        }
    }

    TStringStream ss;
    if (head) {
        bool first = true;
        if (hasName) {
            ss << (first? TString(): sep) << "Name";
            first = false;
        }
        for (const TString& c : cols) {
            ss << (first? TString(): sep) << c;
            first = false;
        }
        ss << Endl;
    }

    for (const TRow& row : in) {
        bool first = true;
        if (hasName) {
            ss << (first? TString(): sep) << row.Name;
            first = false;
        }
        for (const TString& c : cols) {
            ss << (first? TString(): sep);
            first = false;
            TString value;
            ss << (row.GetAsString(c, value) ? value : TString("-"));
        }
        ss << Endl;
    }
    return ss.Str();
}

}
