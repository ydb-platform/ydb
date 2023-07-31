#pragma once

/**
 * @file
 * @brief Strong (because it works) and independent (of contrib/libs/geohash) GeoHash implementation
 * GeoHash algo: https://en.wikipedia.org/wiki/Geohash
 * Useful links:
 * 1. http://geohash.org - Main Site
 * 2. https://dou.ua/lenta/articles/geohash - Geohash-based geopoints clusterization
 * 3. http://www.movable-type.co.uk/scripts/geohash.html - bidirectional encoding and visualization
 */
#include <library/cpp/geohash/direction.h>
#include <library/cpp/geohash/direction.h_serialized.h>

#include <library/cpp/geo/geo.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>

namespace NGeoHash {
    using TBoundingBoxLL = NGeo::TGeoBoundingBox;
    static constexpr auto directionsCount = GetEnumItemsCount<EDirection>();

    template <class T>
    class TNeighbours: public std::array<T, directionsCount> {
    public:
        TNeighbours() = default;

        TNeighbours(std::initializer_list<T> list) {
            Y_ASSERT(list.size() == directionsCount);
            std::copy(list.begin(), list.end(), std::array<T, directionsCount>::begin());
        }

        const T& operator[](EDirection direction) const {
            return std::array<T, directionsCount>::operator[](static_cast<size_t>(direction));
        }

        T& operator[](EDirection direction) {
            return std::array<T, directionsCount>::operator[](static_cast<size_t>(direction));
        }
    };

    class TGeoHashDescriptor {
    public:
        TGeoHashDescriptor() noexcept
            : Bits(0)
            , Steps(0)
        {
        }

        TGeoHashDescriptor(ui64 bits, ui8 steps) noexcept
            : Bits(bits)
            , Steps(steps)
        {
        }

        TGeoHashDescriptor(double latitude, double longitude, ui8 steps);
        TGeoHashDescriptor(double latitude, double longitude, const TBoundingBoxLL& limits, ui8 steps);
        TGeoHashDescriptor(const NGeo::TPointLL& point, ui8 steps);
        TGeoHashDescriptor(const NGeo::TPointLL& point, const TBoundingBoxLL& limits, ui8 steps);

        explicit TGeoHashDescriptor(const TString& hashString);

        ui64 GetBits() const;
        ui8 GetSteps() const;

        TString ToString() const;

        NGeo::TPointLL ToPoint(const TBoundingBoxLL& limits) const;
        NGeo::TPointLL ToPoint() const;

        TBoundingBoxLL ToBoundingBox(const TBoundingBoxLL& limits) const;
        TBoundingBoxLL ToBoundingBox() const;

        TMaybe<TGeoHashDescriptor> GetNeighbour(EDirection direction) const;
        TNeighbours<TMaybe<TGeoHashDescriptor>> GetNeighbours() const;

        TVector<TGeoHashDescriptor> GetChildren(ui8 steps) const;

        static ui8 StepsToPrecision(ui8 steps);
        static ui8 PrecisionToSteps(ui8 precision);

    private:
        void InitFromLatLon(double latitude, double longitude, const TBoundingBoxLL& limits, ui8 steps);
        std::pair<ui8, ui8> LatLonSteps() const;
        std::pair<ui32, ui32> LatLonBits() const;
        void SetLatLonBits(ui32 latBits, ui32 lonBits);
        static ui64 Interleave64(ui32 x, ui32 y);
        static std::pair<ui32, ui32> Deinterleave64(ui64 interleaved);

    private:
        static const ui8 StepsPerPrecisionUnit = 5;
        ui64 Bits;
        ui8 Steps;
    };

    ui64 Encode(double latitude, double longitude, ui8 precision);
    ui64 Encode(const NGeo::TPointLL& point, ui8 precision);

    TString EncodeToString(double latitude, double longitude, ui8 precision);
    TString EncodeToString(const NGeo::TPointLL& point, ui8 precision);

    NGeo::TPointLL DecodeToPoint(const TString& hashString);
    NGeo::TPointLL DecodeToPoint(ui64 hash, ui8 precision);

    TBoundingBoxLL DecodeToBoundingBox(const TString& hashString);
    TBoundingBoxLL DecodeToBoundingBox(ui64 hash, ui8 precision);

    TMaybe<ui64> GetNeighbour(ui64 hash, EDirection direction, ui8 precision);
    TMaybe<TString> GetNeighbour(const TString& hashString, EDirection direction);

    using TGeoHashBitsNeighbours = TNeighbours<TMaybe<ui64>>;
    using TGeoHashStringNeighbours = TNeighbours<TMaybe<TString>>;

    TGeoHashBitsNeighbours GetNeighbours(ui64 hash, ui8 precision);
    TGeoHashStringNeighbours GetNeighbours(const TString& hashString);

    TVector<TString> GetChildren(const TString& hashString);

} /* namespace NGeoHash */
