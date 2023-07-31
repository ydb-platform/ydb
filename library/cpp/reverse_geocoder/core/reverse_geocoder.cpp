#include "reverse_geocoder.h"
#include "geo_data/geo_data.h"

#include <library/cpp/reverse_geocoder/library/unaligned_iter.h>

#include <util/generic/algorithm.h>
#include <util/system/unaligned_mem.h>

using namespace NReverseGeocoder;

static bool PolygonContains(const TPolygon& p, const TPoint& point, const IGeoData& geoData) {
    const TPart* parts = geoData.Parts();
    const TRef* edgeRefs = geoData.EdgeRefs();
    const TEdge* edges = geoData.Edges();
    const TPoint* points = geoData.Points();
    return p.Contains(point, parts, edgeRefs, edges, points);
}

template <typename TAnswer>
static void UpdateAnswer(const TAnswer** answer, const TAnswer& polygon,
                         const IGeoData& geoData) {
    if (!*answer) {
        *answer = &polygon;
    } else {
        const TRegion* regions = geoData.Regions();
        const TNumber regionsNumber = geoData.RegionsNumber();
        if (!(*answer)->Better(polygon, regions, regionsNumber))
            *answer = &polygon;
    }
}

static void SortDebug(TReverseGeocoder::TDebug* debug, const IGeoData& geoData) {
    const TRegion* regions = geoData.Regions();
    const TNumber regionsNumber = geoData.RegionsNumber();

    auto cmp = [&](const TGeoId& a, const TGeoId& b) {
        const TRegion* r1 = LowerBound(regions, regions + regionsNumber, a);
        const TRegion* r2 = LowerBound(regions, regions + regionsNumber, b);
        return r1->Better(*r2);
    };

    Sort(debug->begin(), debug->end(), cmp);
}

TGeoId NReverseGeocoder::TReverseGeocoder::Lookup(const TLocation& location, TDebug* debug) const {
    const IGeoData& geoData = *GeoDataProxy_->GeoData();

    if (debug)
        debug->clear();

    const TPoint point(location);
    const TRef boxRef = LookupAreaBox(point);

    if (boxRef >= geoData.BoxesNumber())
        return UNKNOWN_GEO_ID;

    const TNumber refsOffset = geoData.Boxes()[boxRef].PolygonRefsOffset;
    const TNumber refsNumber = geoData.Boxes()[boxRef].PolygonRefsNumber;

    const TPolygon* answer = nullptr;

    const TPolygon* p = geoData.Polygons();
    const auto refsBegin = UnalignedIter(geoData.PolygonRefs()) + refsOffset;
    const auto refsEnd = refsBegin + refsNumber;

    for (auto iterL = refsBegin, iterR = refsBegin; iterL < refsEnd; iterL = iterR) {
        iterR = iterL + 1;

        if (PolygonContains(p[*iterL], point, geoData)) {
            if (p[*iterL].Type == TPolygon::TYPE_INNER) {
                // All polygons with same RegionId must be skipped if polygon is inner.
                // In geoData small inner polygons stored before big outer polygons.
                while (iterR < refsEnd && p[*iterL].RegionId == p[*iterR].RegionId)
                    ++iterR;

            } else {
                UpdateAnswer(&answer, p[*iterL], geoData);

                if (debug)
                    debug->push_back(p[*iterL].RegionId);

                while (iterR < refsEnd && p[*iterL].RegionId == p[*iterR].RegionId)
                    ++iterR;
            }
        }
    }

    if (debug)
        SortDebug(debug, geoData);

    return answer ? answer->RegionId : UNKNOWN_GEO_ID;
}

TGeoId NReverseGeocoder::TReverseGeocoder::RawLookup(const TLocation& location, TDebug* debug) const {
    const IGeoData& geoData = *GeoDataProxy_->GeoData();

    if (debug)
        debug->clear();

    const TPoint point(location);

    const TRawPolygon* borders = geoData.RawPolygons();
    const TNumber bordersNumber = geoData.RawPolygonsNumber();

    const TRawPolygon* answer = nullptr;

    TNumber i = 0;
    while (i < bordersNumber) {
        if (borders[i].Contains(point, geoData.RawEdgeRefs(), geoData.Edges(), geoData.Points())) {
            if (borders[i].Type == TRawPolygon::TYPE_INNER) {
                TNumber j = i + 1;
                while (j < bordersNumber && borders[i].RegionId == borders[j].RegionId)
                    ++j;

                i = j;

            } else {
                UpdateAnswer(&answer, borders[i], geoData);

                if (debug)
                    debug->push_back(borders[i].RegionId);

                TNumber j = i + 1;
                while (j < bordersNumber && borders[i].RegionId == borders[j].RegionId)
                    ++j;

                i = j;
            }
        } else {
            ++i;
        }
    }

    if (debug)
        SortDebug(debug, geoData);

    return answer ? answer->RegionId : UNKNOWN_GEO_ID;
}

bool NReverseGeocoder::TReverseGeocoder::EachKv(TGeoId regionId, TKvCallback callback) const {
    const IGeoData& g = *GeoDataProxy_->GeoData();

    const TRegion* begin = g.Regions();
    const TRegion* end = begin + g.RegionsNumber();

    const TRegion* region = LowerBound(begin, end, regionId);

    if (region == end || region->RegionId != regionId)
        return false;

    const TKv* kvs = g.Kvs() + region->KvsOffset;
    const char* blobs = g.Blobs();

    for (TNumber i = 0; i < region->KvsNumber; ++i) {
        const char* k = blobs + kvs[i].K;
        const char* v = blobs + kvs[i].V;
        callback(k, v);
    }

    return true;
}

void NReverseGeocoder::TReverseGeocoder::EachPolygon(TPolygonCallback callback) const {
    const IGeoData& g = *GeoDataProxy_->GeoData();

    for (TNumber i = 0; i < g.PolygonsNumber(); ++i)
        callback(g.Polygons()[i]);
}

void NReverseGeocoder::TReverseGeocoder::EachPart(const TPolygon& polygon, TPartCallback callback) const {
    const IGeoData& g = *GeoDataProxy_->GeoData();

    const TNumber partsOffset = polygon.PartsOffset;
    const TNumber partsNumber = polygon.PartsNumber;

    for (TNumber i = partsOffset; i < partsOffset + partsNumber; ++i) {
        const TPart& part = g.Parts()[i];
        const TPart& npart = g.Parts()[i + 1];
        const TNumber edgeRefsNumber = npart.EdgeRefsOffset - part.EdgeRefsOffset;
        callback(part, edgeRefsNumber);
    }
}
