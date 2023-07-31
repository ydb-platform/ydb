#pragma once

#include "point.h"
#include "size.h"
#include "window.h"

#include <util/generic/ymath.h>

namespace NGeo {
    constexpr double MIN_LATITUDE = -90.;
    constexpr double MAX_LATITUDE = +90.;
    constexpr double MIN_LONGITUDE = -180.;
    constexpr double MAX_LONGITUDE = +180.;
    constexpr double WORLD_WIDTH = MAX_LONGITUDE - MIN_LONGITUDE;
    constexpr double WORLD_HEIGHT = MAX_LATITUDE - MIN_LATITUDE;

    // The Mercator projection is truncated at certain latitude so that the visible world forms a square. The poles are not shown.
    constexpr double VISIBLE_LATITUDE_BOUND = 85.084059050109785;

    inline double Deg2rad(double d) {
        return d * PI / 180;
    }

    inline double Rad2deg(double d) {
        return d * 180 / PI;
    }

    inline double GetLongitudeFromMetersAtEquator(double meters) {
        return Rad2deg(meters * (1. / WGS84::R));
    }

    inline double GetMetersFromDeg(double angle) {
        return Deg2rad(angle) * NGeo::WGS84::R;
    }

    inline double GetLatCos(double latDegree) {
        return cos(Deg2rad(latDegree));
    }

    /**
     * Get Inversed cosinus of latitude
     * It is more precise, than division of two big doubles
     * It is safe for lattitue at 90 degrees
     */
    inline double GetInversedLatCosSafe(double latDegree) {
        return 1. / Max(0.001, cos(Deg2rad(latDegree)));
    }

    /**
     * Gets Lontitude width for given width at equator and latitude
     */
    inline double GetWidthAtLatitude(double widthEquator, double latDegree) {
        return widthEquator * GetInversedLatCosSafe(latDegree);
    }

    inline double GetWidthAtLatitude(double widthEquator, const TGeoPoint& p) {
        return GetWidthAtLatitude(widthEquator, p.Lat());
    }

    /*
     * Returns Normalised width at equator for specified width at latitude and latitude
     */

    inline double GetWidthAtEquator(double widthAtLatitude, double latDegree) {
        return widthAtLatitude * GetLatCos(latDegree);
    }

    inline double GetWidthAtEquator(double widthAtLatitude, const TGeoPoint& p) {
        return GetWidthAtEquator(widthAtLatitude, p.Lat());
    }

    /*
     * Same for size
     */

    inline TSize GetSizeAtLatitude(const TSize& sizeAtEquator, const TGeoPoint& at) {
        return TSize(GetWidthAtLatitude(sizeAtEquator.GetWidth(), at), sizeAtEquator.GetHeight());
    }

    inline TSize GetSizeAtEquator(const TSize& sizeAtLatitude, const TGeoPoint& at) {
        return TSize(GetWidthAtEquator(sizeAtLatitude.GetWidth(), at), sizeAtLatitude.GetHeight());
    }

    inline TGeoWindow ConstructWindowFromEquatorSize(const TGeoPoint& center, const TSize& sizeAtEquator) {
        return TGeoWindow(center, GetSizeAtLatitude(sizeAtEquator, center));
    }

    inline double SquaredDiagonal(const NGeo::TSize& size, double latitude) {
        return Sqr(NGeo::GetWidthAtEquator(size.GetWidth(), latitude)) + Sqr(size.GetHeight());
    }

    inline double Diagonal(const NGeo::TSize& size, double latitude) {
        return sqrt(SquaredDiagonal(size, latitude));
    }

    /**
     * try to parse two coords from string
     * return pair of coords on success, otherwise throw exception
     */
    std::pair<double, double> PairFromString(TStringBuf inputStr, TStringBuf delimiter = TStringBuf(","));

    /**
     * try to parse two coords from string
     * write result to first param and return true on success, otherwise return false
     */
    bool TryPairFromString(std::pair<double, double>& res, TStringBuf inputStr, TStringBuf delimiter = TStringBuf(","));
} // namespace NGeo
