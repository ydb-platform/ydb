#include "point.h"
#include "util.h"

#include <util/generic/ylimits.h>
#include <util/generic/ymath.h>

#include <cstdlib>
#include <utility>

namespace NGeo {
    namespace {
        bool IsNonDegeneratePoint(double lon, double lat) {
            return (MIN_LONGITUDE - WORLD_WIDTH < lon && lon < MAX_LONGITUDE + WORLD_WIDTH) &&
                   (MIN_LATITUDE < lat && lat < MAX_LATITUDE);
        }
    } // namespace

    float TGeoPoint::Distance(const TGeoPoint& p) const noexcept {
        auto dp = p - (*this);
        return sqrtf(Sqr(GetWidthAtEquator(dp.GetWidth(), (Lat_ + p.Lat()) * 0.5)) + Sqr(dp.GetHeight()));
    }

    bool TGeoPoint::IsPole() const noexcept {
        return Lat_ <= MIN_LATITUDE || MAX_LATITUDE <= Lat_;
    }

    bool TGeoPoint::IsVisibleOnMap() const noexcept {
        return -VISIBLE_LATITUDE_BOUND <= Lat_ && Lat_ <= VISIBLE_LATITUDE_BOUND;
    }

    TGeoPoint TGeoPoint::Parse(TStringBuf s, TStringBuf delimiter) {
        const auto& [lon, lat] = PairFromString(s, delimiter);
        Y_ENSURE_EX(IsNonDegeneratePoint(lon, lat), TBadCastException() << "Invalid point: (" << lon << ", " << lat << ")");
        return {lon, lat};
    }

    TMaybe<TGeoPoint> TGeoPoint::TryParse(TStringBuf s, TStringBuf delimiter) {
        std::pair<double, double> lonLat;
        if (!TryPairFromString(lonLat, s, delimiter)) {
            return {};
        }
        if (!IsNonDegeneratePoint(lonLat.first, lonLat.second)) {
            return {};
        }
        return TGeoPoint(lonLat.first, lonLat.second);
    }

    TSize operator-(const TGeoPoint& p1, const TGeoPoint& p2) {
        return {p1.Lon() - p2.Lon(), p1.Lat() - p2.Lat()};
    }

    /*
    Conversion code was imported from http://wiki.yandex-team.ru/YandexMobile/maps/Algorithm/mapengine/coordtransforms
    */
    namespace WGS84 {
        /* Isometric to geodetic latitude parameters, default to WGS 84 */
        const double ab = 0.00335655146887969400;
        const double bb = 0.00000657187271079536;
        const double cb = 0.00000001764564338702;
        const double db = 0.00000000005328478445;

        const double _a = R;
        const double _f = 1.0 / 298.257223563;
        const double _b = _a - _f * _a;
        const double _e = sqrt(1 - pow(_b / _a, 2));
        const double _e2 = _e * _e;
        const double _g = sqrt(1.0 - _e2);
        const double _gR2 = _g * R * 2.0;
    } // namespace WGS84

    TGeoPoint MercatorToLL(TMercatorPoint pt) {
        using namespace WGS84;

        // Y_ENSURE(pt.IsDefined(), "Point is not defined");

        /* Isometric latitude*/
        const double xphi = PI / 2.0 - 2.0 * atan(exp(-pt.Y_ / R));

        double latitude = xphi + ab * sin(2.0 * xphi) + bb * sin(4.0 * xphi) + cb * sin(6.0 * xphi) + db * sin(8.0 * xphi);
        double longitude = pt.X_ / R;

        return TGeoPoint{Rad2deg(longitude), Rad2deg(latitude)};
    }

    double GetMercatorY(const TGeoPoint& ll) {
        if (Y_UNLIKELY(ll.Lat() == 0.)) {
            // shortcut for common case, avoiding floating point errors
            return 0.;
        }
        if (Y_UNLIKELY(ll.Lat() == MIN_LATITUDE)) {
            return -std::numeric_limits<double>::infinity();
        }
        if (Y_UNLIKELY(ll.Lat() == MAX_LATITUDE)) {
            return +std::numeric_limits<double>::infinity();
        }
        double lat = Deg2rad(ll.Lat());
        double esinLat = WGS84::_e * sin(lat);

        double tan_temp = tan(PI / 4.e0 + lat / 2.e0);
        double pow_temp = pow(tan(PI / 4.e0 + asin(esinLat) / 2), WGS84::_e);
        double U = tan_temp / pow_temp;
        return WGS84::R * log(U);
    }

    TMercatorPoint LLToMercator(TGeoPoint ll) {
        // Y_ENSURE(ll.IsValid(), "Point is not defined");

        // Y_ENSURE(-90. <= ll.Lat() && ll.Lat() <= +90., "Latitude is out of range [-90, 90]");

        double lon = Deg2rad(ll.Lon());
        double x = WGS84::R * lon;
        double y = GetMercatorY(ll);

        return TMercatorPoint{x, y};
    }

    double GeodeticDistance(TGeoPoint p1, TGeoPoint p2) {
        using namespace WGS84;

        constexpr double deg2HalfRad = PI / 360.0;

        const double lon1Half = p1.Lon() * deg2HalfRad;
        const double lon2Half = p2.Lon() * deg2HalfRad;

        const double lat1Half = p1.Lat() * deg2HalfRad;
        const double lat2Half = p2.Lat() * deg2HalfRad;

        const double diffLatHalf = fabs(lat1Half - lat2Half);
        const double diffLonHalf = fabs(lon1Half - lon2Half);

        if (diffLatHalf < 0.5e-8 && diffLonHalf < 0.5e-8) {
            return 0;
        }

        double s = sin(lat1Half + lat2Half);
        double s2 = s * s;
        double m = _gR2 / (1.0 - _e2 * s2);

        const double w = sin(diffLatHalf);
        const double w2 = w * w;
        const double cc = Max(1.0 - s2 - w2, 0.0); // cos(lat1Half * 2) * cos(lat2Half * 2)
        const double z = sin(diffLonHalf);

        return m * asin(sqrt(w2 + cc * z * z));
    }
} // namespace NGeo
