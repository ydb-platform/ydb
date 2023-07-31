#pragma once

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/generic/maybe.h>

#include <algorithm>
#include <cmath>

namespace NGeo {
    class TSize;

    class TGeoPoint {
    public:
        TGeoPoint(double lon, double lat) noexcept
            : Lon_(lon)
            , Lat_(lat)
        {
        }

        TGeoPoint() noexcept
            : Lon_(BadX)
            , Lat_(BadY)
        {
        }

        double Lon() const noexcept {
            return Lon_;
        }

        double Lat() const noexcept {
            return Lat_;
        }

        float Distance(const TGeoPoint& p) const noexcept;

        void swap(TGeoPoint& p) noexcept {
            std::swap(Lon_, p.Lon_);
            std::swap(Lat_, p.Lat_);
        }

        bool IsValid() const {
            return (Lon_ != BadX) && (Lat_ != BadY);
        }

        /// Returns true if the point represents either North or South Pole
        bool IsPole() const noexcept;

        /// Returns true if the point may be shown on the Yandex Map (fits into the valid range of latitudes)
        bool IsVisibleOnMap() const noexcept;

        bool operator!() const {
            return !IsValid();
        }

        TString ToCgiStr() const {
            return ToString();
        }

        TString ToString(const char* delimiter = ",") const {
            return TString::Join(::ToString(Lon_), delimiter, ::ToString(Lat_));
        }

        /**
         * \note Parsing functions work is safe way. They discard invalid points:
         * 1) on the Poles and 'beyond' the Poles;
         * 2) not belonging to the 'main' world and +/-1 world to the left or to the right.
         * If you need such cases, construct the TGeoPoint manually.
         */

        /// Throws TBadCastException on error
        static TGeoPoint Parse(TStringBuf s, TStringBuf delimiter = TStringBuf(","));

        /// Returns Nothing() on error
        static TMaybe<TGeoPoint> TryParse(TStringBuf s, TStringBuf delimiter = TStringBuf(","));

    private:
        double Lon_;
        double Lat_;

        static constexpr double BadX{361.};
        static constexpr double BadY{181.};
    };

    double GeodeticDistance(TGeoPoint p1, TGeoPoint p2);

    /**
     * \class TMercatorPoint
     *
     * Represents a point in EPSG:3395 projection
     * (WGS 84 / World Mercator)
     */
    class TMercatorPoint {
    public:
        friend class TMercatorWindow;
        friend TGeoPoint MercatorToLL(TMercatorPoint);

        /**
         * Constructs a point with the given coordinates.
         */
        constexpr TMercatorPoint(double x, double y) noexcept
            : X_{x}
            , Y_{y}
        {
        }

        /**
         * Constructs a point with two NaN coordinates.
         *
         * Should not be called directly.
         * If your `point` variable might be undefined,
         * declare it explicitly as TMaybe<TMercatorPoint>.
         */
        constexpr TMercatorPoint() noexcept
            : X_{std::numeric_limits<double>::quiet_NaN()}
            , Y_{std::numeric_limits<double>::quiet_NaN()}
        {
        }

        /**
         * Returns the X_ coordinate.
         *
         * The line X_ == 0 corresponds to the Prime meridian.
         */
        constexpr double X() const noexcept {
            return X_;
        }

        /**
         * Returns the Y_ coordinate.
         *
         * The line Y_ == 0 corresponds to the Equator.
         */
        constexpr double Y() const noexcept {
            return Y_;
        }

    private:
        bool IsDefined() const noexcept {
            return !std::isnan(X_) && !std::isnan(Y_);
        }

    private:
        double X_;
        double Y_;
    };

    /**
     * Operators
     */

    inline bool operator==(const TGeoPoint& p1, const TGeoPoint& p2) {
        return p1.Lon() == p2.Lon() && p1.Lat() == p2.Lat();
    }

    inline bool operator==(const TMercatorPoint& p1, const TMercatorPoint& p2) {
        return p1.X() == p2.X() && p1.Y() == p2.Y();
    }

    inline bool operator<(const TGeoPoint& p1, const TGeoPoint& p2) {
        if (p1.Lon() != p2.Lon()) {
            return p1.Lon() < p2.Lon();
        }
        return p1.Lat() < p2.Lat();
    }

    /**
     * Conversion
     */

    namespace WGS84 {
        /* Radius of reference ellipsoid, default to WGS 84 */
        const double R = 6378137.0;
    } // namespace WGS84

    using TPointLL = TGeoPoint;
    using TPointXY = TMercatorPoint;

    TGeoPoint MercatorToLL(TMercatorPoint);
    TMercatorPoint LLToMercator(TGeoPoint);

    /**
     * Input/output
     */

    TSize operator-(const TGeoPoint& p1, const TGeoPoint& p2);
} // namespace NGeo

template <>
inline void Out<NGeo::TGeoPoint>(IOutputStream& o, const NGeo::TGeoPoint& p) {
    o << '[' << p.Lon() << ", " << p.Lat() << ']';
}

template <>
inline void Out<NGeo::TMercatorPoint>(IOutputStream& o, const NGeo::TMercatorPoint& p) {
    o << '[' << p.X() << ", " << p.Y() << ']';
}
