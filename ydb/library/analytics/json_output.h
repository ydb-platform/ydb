#pragma once

#include <util/string/printf.h>
#include <util/stream/str.h>
#include <util/string/vector.h>
#include <util/generic/set.h>
#include <util/generic/hash_set.h>
#include "data.h"
#include "util.h"

namespace NAnalytics {

inline TString ToJsonFlot(const TTable& in, const TString& xno, const TVector<TString>& ynos, const TString& opts = TString())
{
    TStringStream ss;
    ss << "[ ";
    bool first = true;

    TString xn;
    THashSet<TString> xopts;
    ParseNameAndOpts(xno, xn, xopts);
    bool xstack = xopts.contains("stack");

    for (const TString& yno : ynos) {
        TString yn;
        THashSet<TString> yopts;
        ParseNameAndOpts(yno, yn, yopts);
        bool ystackOpt = yopts.contains("stack");

        ss << (first? "": ",\n  ") <<  "{ " << opts << (opts? ", ": "") << "\"label\": \"" << yn << "\", \"data\": [ ";
        bool first2 = true;
        using TPt = std::tuple<double, double, TString>;
        std::vector<TPt> pts;
        for (const TRow& row : in) {
            double x, y;
            if (row.Get(xn, x) && row.Get(yn, y)) {
                pts.emplace_back(x, y, row.Name);
            }
        }

        if (xstack) {
            std::sort(pts.begin(), pts.end(), [] (const TPt& a, const TPt& b) {
                // At first sort by Name, then by x, then by y
                return std::make_tuple(std::get<2>(a), std::get<0>(a), std::get<1>(a)) <
                       std::make_tuple(std::get<2>(b), std::get<0>(b), std::get<1>(b));
            });
        } else {
            std::sort(pts.begin(), pts.end());
        }

        double x = 0.0, xsum = 0.0;
        double y = 0.0, ysum = 0.0;
        for (auto& pt : pts) {
            if (xstack) {
                x = xsum;
                xsum += std::get<0>(pt);
            } else {
                x = std::get<0>(pt);
            }

            if (ystackOpt) {
                y = ysum;
                ysum += std::get<1>(pt);
            } else {
                y = std::get<1>(pt);
            }

            ss << (first2? "": ", ") << "["
               << Sprintf("%.6lf", Finitize(x)) << ", "     // x coordinate
               << Sprintf("%.6lf", Finitize(y)) << ", "     // y coordinate
               << "\"" << std::get<2>(pt) << "\", "       // label
               << Sprintf("%.6lf", std::get<0>(pt)) << ", " // x label (real value)
               << Sprintf("%.6lf", std::get<1>(pt))         // y label (real value)
               << "]";
            first2 = false;
        }
        // Add final point
        if (!first2 && (xstack || ystackOpt)) {
            if (xstack)
                x = xsum;
            if (ystackOpt)
                y = ysum;
            ss << (first2? "": ", ") << "["
               << Sprintf("%.6lf", Finitize(x)) << ", " // x coordinate
               << Sprintf("%.6lf", Finitize(y)) << ", " // y coordinate
               << "\"\", "
               << Sprintf("%.6lf", x) << ", "           // x label (real value)
               << Sprintf("%.6lf", y)                   // y label (real value)
               << "]";
        }
        ss << " ] }";
        first = false;
    }
    ss << "\n]";
    return ss.Str();
}

}
