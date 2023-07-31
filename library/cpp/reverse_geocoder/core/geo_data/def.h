#pragma once

#include <library/cpp/reverse_geocoder/core/area_box.h>
#include <library/cpp/reverse_geocoder/core/common.h>
#include <library/cpp/reverse_geocoder/core/edge.h>
#include <library/cpp/reverse_geocoder/core/kv.h>
#include <library/cpp/reverse_geocoder/core/part.h>
#include <library/cpp/reverse_geocoder/core/point.h>
#include <library/cpp/reverse_geocoder/core/polygon.h>
#include <library/cpp/reverse_geocoder/core/region.h>

namespace NReverseGeocoder {
    const TVersion GEO_DATA_VERSION_0 = 0;
    const TVersion GEO_DATA_VERSION_1 = 1;

    const TVersion GEO_DATA_CURRENT_VERSION = GEO_DATA_VERSION_1;

// Geographical data definition. This define need for reflection in map/unmap, show, etc.
#define GEO_BASE_DEF_GEO_DATA                   \
    GEO_BASE_DEF_VAR(TVersion, Version);        \
    GEO_BASE_DEF_ARR(TPoint, Points);           \
    GEO_BASE_DEF_ARR(TEdge, Edges);             \
    GEO_BASE_DEF_ARR(TRef, EdgeRefs);           \
    GEO_BASE_DEF_ARR(TPart, Parts);             \
    GEO_BASE_DEF_ARR(TPolygon, Polygons);       \
    GEO_BASE_DEF_ARR(TRef, PolygonRefs);        \
    GEO_BASE_DEF_ARR(TAreaBox, Boxes);          \
    GEO_BASE_DEF_ARR(char, Blobs);              \
    GEO_BASE_DEF_ARR(TKv, Kvs);                 \
    GEO_BASE_DEF_ARR(TRegion, Regions);         \
    GEO_BASE_DEF_ARR(TRawPolygon, RawPolygons); \
    GEO_BASE_DEF_ARR(TRef, RawEdgeRefs);        \
    // #define GEO_BASE_DEF_GEO_DATA

}
