#include "size.h"

#include "util.h"

namespace NGeo {
    const double TSize::BadWidth = -1.;
    const double TSize::BadHeight = -1.;

    namespace {
        bool IsNonNegativeSize(double width, double height) {
            return width >= 0. && height >= 0.;
        }
    } // namespace

    TSize TSize::Parse(TStringBuf s, TStringBuf delimiter) {
        const auto& [width, height] = PairFromString(s, delimiter);
        Y_ENSURE_EX(IsNonNegativeSize(width, height), TBadCastException() << "Negative window size");
        return {width, height};
    }

    TMaybe<TSize> TSize::TryParse(TStringBuf s, TStringBuf delimiter) {
        std::pair<double, double> lonLat;
        if (!TryPairFromString(lonLat, s, delimiter)) {
            return {};
        }
        if (!IsNonNegativeSize(lonLat.first, lonLat.second)) {
            return {};
        }
        return TSize{lonLat.first, lonLat.second};
    }
} // namespace NGeo
