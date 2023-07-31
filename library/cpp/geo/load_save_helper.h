#pragma once

#include <library/cpp/geo/window.h>
#include <util/stream/input.h>
#include <util/ysaveload.h>

template <>
struct TSerializer<NGeo::TGeoPoint> {
    static void Save(IOutputStream*, const NGeo::TGeoPoint&);
    static void Load(IInputStream*, NGeo::TGeoPoint&);
};

template <>
struct TSerializer<NGeo::TGeoWindow> {
    static void Save(IOutputStream*, const NGeo::TGeoWindow&);
    static void Load(IInputStream*, NGeo::TGeoWindow&);
};

template <>
struct TSerializer<NGeo::TSize> {
    static void Save(IOutputStream*, const NGeo::TSize&);
    static void Load(IInputStream*, NGeo::TSize&);
};
