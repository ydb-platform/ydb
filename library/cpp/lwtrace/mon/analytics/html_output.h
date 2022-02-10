#pragma once

#include <util/string/printf.h>
#include <util/stream/str.h>
#include <util/generic/set.h>
#include "data.h"

namespace NAnalytics {

inline TString ToHtml(const TTable& in)
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
    ss << "<table>";
    ss << "<thead><tr>";
    if (hasName) {
        ss << "<th>Name</th>";
    }
    for (const TString& c : cols) {
        ss << "<th>" << c << "</th>";
    }
    ss << "</tr></thead><tbody>";

    for (const TRow& row : in) {
        ss << "<tr>";
        if (hasName) {
            ss << "<th>" << row.Name << "</th>";
        }
        for (const TString& c : cols) {
            TString value;
            ss << "<td>" << (row.GetAsString(c, value) ? value : TString("-")) << "</td>";
        }
        ss << "</tr>";
    }
    ss << "</tbody></table>";

    return ss.Str();
}

inline TString ToTransposedHtml(const TTable& in)
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
    ss << "<table><thead>";
    if (hasName) {
        ss << "<tr>";
        ss << "<th>Name</th>";
        for (const TRow& row : in) {
            ss << "<th>" << row.Name << "</th>";
        }
        ss << "</tr>";
    }

    ss << "</thead><tbody>";

    for (const TString& c : cols) {
        ss << "<tr>";
        ss << "<th>" << c << "</th>";
        for (const TRow& row : in) {
            TString value;
            ss << "<td>" << (row.GetAsString(c, value) ? value : TString("-")) << "</td>";
        }
        ss << "</tr>";
    }
    ss << "</tbody></table>";

    return ss.Str();
}

}
