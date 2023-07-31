#include "util.h"

#include <math.h>
#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <utility>

namespace NGeo {
    bool TryPairFromString(std::pair<double, double>& res, TStringBuf inputStr, TStringBuf delimiter) {
        TStringBuf lhsStr;
        TStringBuf rhsStr;

        double lhs = NAN;
        double rhs = NAN;
        if (
            !inputStr.TrySplit(delimiter, lhsStr, rhsStr) ||
            !TryFromString<double>(lhsStr, lhs) ||
            !TryFromString<double>(rhsStr, rhs)) {
            return false;
        }

        res = {lhs, rhs};
        return true;
    }

    std::pair<double, double> PairFromString(TStringBuf inputStr, TStringBuf delimiter) {
        std::pair<double, double> res;
        if (!TryPairFromString(res, inputStr, delimiter)) {
            ythrow TBadCastException() << "Wrong point string: " << inputStr;
        }
        return res;
    }
} // namespace NGeo
