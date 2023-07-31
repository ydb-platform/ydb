#pragma once

#include "common.h"
#include "geo_data/geo_data.h"
#include "geo_data/proxy.h"

#include <util/generic/noncopyable.h>
#include <util/generic/vector.h>

#include <functional>

namespace NReverseGeocoder {
    const TGeoId UNKNOWN_GEO_ID = static_cast<TGeoId>(-1);

    // NOTE: Be careful! It's work fine and fast on real world dataset.
    //       But in theory it's can spent O(n^2) memory (on real world dataset it's just 6n).
    //       Point in polygon test will be O(log n) always. Memory spent will be O(n) in future!
    class TReverseGeocoder: public TNonCopyable {
    public:
        using TDebug = TVector<TGeoId>;
        using TKvCallback = std::function<void(const char*, const char*)>;
        using TPolygonCallback = std::function<void(const TPolygon&)>;
        using TPartCallback = std::function<void(const TPart&, TNumber)>;

        TReverseGeocoder()
            : GeoDataProxy_()
        {
        }

        TReverseGeocoder(TReverseGeocoder&& g)
            : GeoDataProxy_()
        {
            DoSwap(GeoDataProxy_, g.GeoDataProxy_);
        }

        TReverseGeocoder& operator=(TReverseGeocoder&& g) {
            DoSwap(GeoDataProxy_, g.GeoDataProxy_);
            return *this;
        }

        explicit TReverseGeocoder(const char* path)
            : GeoDataProxy_(new TGeoDataMapProxy(path))
        {
        }

        explicit TReverseGeocoder(const IGeoData& geoData)
            : GeoDataProxy_(new TGeoDataWrapper(geoData))
        {
        }

        TReverseGeocoder(const char* data, size_t dataSize)
            : GeoDataProxy_(new TGeoDataRawProxy(data, dataSize))
        {
        }

        TGeoId Lookup(const TLocation& location, TDebug* debug = nullptr) const;

        TGeoId RawLookup(const TLocation& location, TDebug* debug = nullptr) const;

        bool EachKv(TGeoId regionId, TKvCallback callback) const;

        void EachPolygon(TPolygonCallback callback) const;

        void EachPart(const TPolygon& polygon, TPartCallback callback) const;

        const IGeoData& GeoData() const {
            return *GeoDataProxy_->GeoData();
        }

    private:
        TGeoDataProxyPtr GeoDataProxy_;
    };
}
