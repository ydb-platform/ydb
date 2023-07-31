#include "window.h"

#include "util.h"

#include <util/generic/ylimits.h>
#include <util/generic/ymath.h>
#include <util/generic/maybe.h>

#include <cstdlib>
#include <utility>

namespace NGeo {
    namespace {
        TMercatorPoint GetMiddlePoint(const TMercatorPoint& p1, const TMercatorPoint& p2) {
            return TMercatorPoint{(p1.X() + p2.X()) / 2, (p1.Y() + p2.Y()) / 2};
        }

        struct TLatBounds {
            double LatMin;
            double LatMax;
        };
    } // namespace

    bool TrySpan2LatitudeDegenerateCases(double ll, double lspan, TLatBounds& result) {
        // TODO(sobols@): Compare with eps?
        if (Y_UNLIKELY(lspan >= 180.)) {
            result.LatMin = -90.;
            result.LatMax = +90.;
            return true;
        }
        if (Y_UNLIKELY(ll == +90.)) {
            result.LatMin = ll - lspan;
            result.LatMax = ll;
            return true;
        }
        if (Y_UNLIKELY(ll == -90.)) {
            result.LatMin = ll;
            result.LatMax = ll + lspan;
            return true;
        }
        return false;
    }

    /**
     * Finds such latitudes lmin, lmax that:
     * 1) lmin <= ll <= lmax,
     * 2) lmax - lmin == lspan,
     * 3) MercatorY(ll) - MercatorY(lmin) == MercatorY(lmax) - MercatorY(ll)
     * (the ll parallel is a center between lmin and lmax parallels in Mercator projection)
     *
     * \returns a pair (lmin, lmax)
     */
    TLatBounds Span2Latitude(double ll, double lspan) {
        TLatBounds result{};
        if (TrySpan2LatitudeDegenerateCases(ll, lspan, result)) {
            return result;
        }

        const double lc = Deg2rad(ll);
        const double h = Deg2rad(lspan);

        // Spherical (Pseudo) Mercator:
        //   MercatorY(lc) = R * ln(tan(lc / 2 + PI / 4)).
        // Note that
        //   ln(a) - ln(b) = ln(a / b)
        // That'a why
        //   MercatorY(lc) - MercatorY(lmin) == MercatorY(lmin + h) - MercatorY(lc) <=>
        //   <=> tan(lc / 2 + PI / 4) / tan(lmin / 2 + PI / 4) ==
        //   == tan(lmin / 2 + h / 2 + PI / 4) / tan(lc / 2 + PI / 4).
        // Also note that
        //   tan(x + y) == (tan(x) + tan(y)) / (1 - tan(x) * tan(y)),
        // so
        //   tan(lmin / 2 + h / 2 + PI / 4) ==
        //   == (tan(lmin / 2 + PI / 4) + tan(h / 2)) / (1 - tan(lmin / 2 + PI / 4) * tan(h / 2))

        const double yx = tan(lc / 2 + PI / 4);

        // Let x be tan(lmin / 2 + PI / 4),
        // then
        //   yx / x == (x + tan(h / 2)) / ((1 - x * tan(h / 2)) * yx),
        // or
        //   yx^2 * (1 - x * tan(h / 2)) == (x + tan(h / 2)) * x.
        // Now we solve a quadratic equation:
        //   x^2 + bx + c == 0

        const double C = yx * yx;

        const double b = (C + 1) * tan(h / 2), c = -C;
        const double D = b * b - 4 * c;
        const double root = (-b + sqrt(D)) / 2;

        result.LatMin = Rad2deg((atan(root) - PI / 4) * 2);
        result.LatMax = result.LatMin + lspan;
        return result;
    }

    void TGeoWindow::CalcCorners() {
        if (!IsValid()) {
            return;
        }
        const TLatBounds latBounds = Span2Latitude(Center_.Lat(), Size_.GetHeight());

        if (-90. < latBounds.LatMin && latBounds.LatMax < +90.) {
            TMercatorPoint lowerLeftCornerM = LLToMercator(TGeoPoint(Center_.Lon() - (Size_.GetWidth() / 2), latBounds.LatMin));
            TMercatorPoint upperRightCornerM = LLToMercator(TGeoPoint(Center_.Lon() + (Size_.GetWidth() / 2), latBounds.LatMax));
            TMercatorPoint centerM = LLToMercator(Center_);

            double w = upperRightCornerM.X() - lowerLeftCornerM.X();
            double h = upperRightCornerM.Y() - lowerLeftCornerM.Y();

            LowerLeftCorner_ = MercatorToLL(TMercatorPoint(centerM.X() - w / 2, centerM.Y() - h / 2));
            UpperRightCorner_ = MercatorToLL(TMercatorPoint(centerM.X() + w / 2, centerM.Y() + h / 2));
        } else {
            LowerLeftCorner_ = TGeoPoint(Center_.Lon() - (Size_.GetWidth() / 2), latBounds.LatMin);
            UpperRightCorner_ = TGeoPoint(Center_.Lon() + (Size_.GetWidth() / 2), latBounds.LatMax);
        }
    }

    void TGeoWindow::CalcCenterAndSpan() {
        if (!LowerLeftCorner_ || !UpperRightCorner_) {
            return;
        }

        TMercatorPoint lower = LLToMercator(LowerLeftCorner_);
        TMercatorPoint upper = LLToMercator(UpperRightCorner_);
        TMercatorPoint center = GetMiddlePoint(lower, upper);
        Center_ = MercatorToLL(center);

        Size_ = TSize(UpperRightCorner_.Lon() - LowerLeftCorner_.Lon(),
                      UpperRightCorner_.Lat() - LowerLeftCorner_.Lat());
    }

    bool TGeoWindow::Contains(const TGeoPoint& p) const {
        return LowerLeftCorner_.Lon() <= p.Lon() && p.Lon() <= UpperRightCorner_.Lon() &&
               LowerLeftCorner_.Lat() <= p.Lat() && p.Lat() <= UpperRightCorner_.Lat();
    }

    double TGeoWindow::Diameter() const {
        return Diagonal(Size_, Center_.Lat());
    }

    double TGeoWindow::Distance(const TGeoWindow& w) const {
        const double minX = Max(GetLowerLeftCorner().Lon(), w.GetLowerLeftCorner().Lon());
        const double maxX = Min(GetUpperRightCorner().Lon(), w.GetUpperRightCorner().Lon());
        const double minY = Max(GetLowerLeftCorner().Lat(), w.GetLowerLeftCorner().Lat());
        const double maxY = Min(GetUpperRightCorner().Lat(), w.GetUpperRightCorner().Lat());
        double xGap = minX > maxX ? (minX - maxX) : 0.;
        double yGap = minY > maxY ? (minY - maxY) : 0.;
        return sqrtf(Sqr(xGap * cos((minY + maxY) * 0.5 * PI / 180)) + Sqr(yGap));
    }

    double TWindowLL::GetApproxDistance(const TPointLL& point) const {
        const double metresInDegree = WGS84::R * PI / 180;
        return Distance(TWindowLL{point, point}) * metresInDegree;
    }

    TGeoWindow TGeoWindow::ParseFromCornersPoints(TStringBuf leftCornerStr, TStringBuf rightCornerStr, TStringBuf delimiter) {
        auto leftCorner = TGeoPoint::Parse(leftCornerStr, delimiter);
        auto rightCorner = TGeoPoint::Parse(rightCornerStr, delimiter);

        return {leftCorner, rightCorner};
    }

    TMaybe<TGeoWindow> TGeoWindow::TryParseFromCornersPoints(TStringBuf leftCornerStr, TStringBuf rightCornerStr, TStringBuf delimiter) {
        auto leftCorner = TGeoPoint::TryParse(leftCornerStr, delimiter);
        auto rightCorner = TGeoPoint::TryParse(rightCornerStr, delimiter);
        if (!leftCorner || !rightCorner) {
            return {};
        }

        return TGeoWindow{*leftCorner, *rightCorner};
    }

    TGeoWindow TGeoWindow::ParseFromLlAndSpn(TStringBuf llStr, TStringBuf spnStr, TStringBuf delimiter) {
        TGeoPoint ll = TGeoPoint::Parse(llStr, delimiter);
        TSize spn = TSize::Parse(spnStr, delimiter);

        return {ll, spn};
    }

    TMaybe<TGeoWindow> TGeoWindow::TryParseFromLlAndSpn(TStringBuf llStr, TStringBuf spnStr, TStringBuf delimiter) {
        auto ll = TGeoPoint::TryParse(llStr, delimiter);
        auto spn = TSize::TryParse(spnStr, delimiter);

        if (!ll || !spn) {
            return {};
        }

        return TGeoWindow{*ll, *spn};
    }
    /**
     * TMercatorWindow
     */

    TMercatorWindow::TMercatorWindow() noexcept
        : HalfWidth_{std::numeric_limits<double>::quiet_NaN()}
        , HalfHeight_{std::numeric_limits<double>::quiet_NaN()}
    {
    }

    TMercatorWindow::TMercatorWindow(const TMercatorPoint& center, const TSize& size) noexcept
        : Center_{center}
        , HalfWidth_{size.GetWidth() / 2}
        , HalfHeight_{size.GetHeight() / 2}
    {
    }

    TMercatorWindow::TMercatorWindow(const TMercatorPoint& firstPoint, const TMercatorPoint& secondPoint) noexcept
        : Center_{GetMiddlePoint(firstPoint, secondPoint)}
        , HalfWidth_{Abs(secondPoint.X() - firstPoint.X()) / 2}
        , HalfHeight_{Abs(secondPoint.Y() - firstPoint.Y()) / 2}
    {
    }

    bool TMercatorWindow::Contains(const TMercatorPoint& pt) const noexcept {
        return (Center_.X() - HalfWidth_ <= pt.X()) &&
               (pt.X() <= Center_.X() + HalfWidth_) &&
               (Center_.Y() - HalfHeight_ <= pt.Y()) &&
               (pt.Y() <= Center_.Y() + HalfHeight_);
    }

    /**
     * Conversion
     */

    TMercatorWindow LLToMercator(const TGeoWindow& window) {
        return TMercatorWindow{LLToMercator(window.GetLowerLeftCorner()), LLToMercator(window.GetUpperRightCorner())};
    }

    TGeoWindow MercatorToLL(const TMercatorWindow& window) {
        return TGeoWindow{MercatorToLL(window.GetLowerLeftCorner()), MercatorToLL(window.GetUpperRightCorner())};
    }

    /**
     * Operators
     */

    TMaybe<TGeoWindow> Intersection(const TGeoWindow& lhs, const TGeoWindow& rhs) {
        const double minX = Max(lhs.GetLowerLeftCorner().Lon(), rhs.GetLowerLeftCorner().Lon());
        const double maxX = Min(lhs.GetUpperRightCorner().Lon(), rhs.GetUpperRightCorner().Lon());
        const double minY = Max(lhs.GetLowerLeftCorner().Lat(), rhs.GetLowerLeftCorner().Lat());
        const double maxY = Min(lhs.GetUpperRightCorner().Lat(), rhs.GetUpperRightCorner().Lat());
        if (minX > maxX || minY > maxY) {
            return {};
        }
        return TGeoWindow(TGeoPoint(minX, minY), TGeoPoint(maxX, maxY));
    }

    TMaybe<TGeoWindow> Intersection(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs) {
        if (!lhs || !rhs) {
            return {};
        }
        return Intersection(*lhs, *rhs);
    }

    TGeoWindow Union(const TGeoWindow& lhs, const TGeoWindow& rhs) {
        const double minX = Min(lhs.GetLowerLeftCorner().Lon(), rhs.GetLowerLeftCorner().Lon());
        const double maxX = Max(lhs.GetUpperRightCorner().Lon(), rhs.GetUpperRightCorner().Lon());
        const double minY = Min(lhs.GetLowerLeftCorner().Lat(), rhs.GetLowerLeftCorner().Lat());
        const double maxY = Max(lhs.GetUpperRightCorner().Lat(), rhs.GetUpperRightCorner().Lat());
        return TGeoWindow{TGeoPoint{minX, minY}, TGeoPoint{maxX, maxY}};
    }

    TMaybe<TGeoWindow> Union(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs) {
        if (!lhs) {
            return rhs;
        }
        if (!rhs) {
            return lhs;
        }
        return Union(*lhs, *rhs);
    }

    bool Contains(const TMaybe<TGeoWindow>& window, const TGeoPoint& point) {
        if (!window) {
            return false;
        }
        return window.GetRef().Contains(point);
    }

    bool Intersects(const TGeoWindow& lhs, const TGeoWindow& rhs) {
        bool haveHorizIntersection =
            !(lhs.GetUpperRightCorner().Lon() <= rhs.GetLowerLeftCorner().Lon() ||
              rhs.GetUpperRightCorner().Lon() <= lhs.GetLowerLeftCorner().Lon());
        bool haveVertIntersection =
            !(lhs.GetUpperRightCorner().Lat() <= rhs.GetLowerLeftCorner().Lat() ||
              rhs.GetUpperRightCorner().Lat() <= lhs.GetLowerLeftCorner().Lat());
        return haveHorizIntersection && haveVertIntersection;
    }

    bool Intersects(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs) {
        if (!lhs || !rhs) {
            return false;
        }
        return Intersects(*lhs, *rhs);
    }
} // namespace NGeo
