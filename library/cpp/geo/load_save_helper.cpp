#include "load_save_helper.h"
#include <util/stream/input.h>

void TSerializer<NGeo::TGeoPoint>::Save(IOutputStream* out, const NGeo::TGeoPoint& point) {
    double lon = static_cast<double>(point.Lon());
    double lat = static_cast<double>(point.Lat());
    ::Save(out, lon);
    ::Save(out, lat);
}

void TSerializer<NGeo::TGeoPoint>::Load(IInputStream* in, NGeo::TGeoPoint& point) {
    double lon = std::numeric_limits<double>::quiet_NaN();
    double lat = std::numeric_limits<double>::quiet_NaN();
    ::Load(in, lon);
    ::Load(in, lat);
    point = {lon, lat};
}

void TSerializer<NGeo::TGeoWindow>::Save(IOutputStream* out, const NGeo::TGeoWindow& window) {
    const auto& center = window.GetCenter();
    const auto& size = window.GetSize();
    ::Save(out, center);
    ::Save(out, size);
}

void TSerializer<NGeo::TGeoWindow>::Load(IInputStream* in, NGeo::TGeoWindow& window) {
    NGeo::TSize size{};
    NGeo::TGeoPoint center{};

    ::Load(in, center);
    ::Load(in, size);

    window = {center, size};
}

void TSerializer<NGeo::TSize>::Save(IOutputStream* out, const NGeo::TSize& size) {
    double width = static_cast<double>(size.GetWidth());
    double height = static_cast<double>(size.GetHeight());
    ::Save(out, width);
    ::Save(out, height);
}

void TSerializer<NGeo::TSize>::Load(IInputStream* in, NGeo::TSize& size) {
    double width = std::numeric_limits<double>::quiet_NaN();
    double height = std::numeric_limits<double>::quiet_NaN();
    ::Load(in, width);
    ::Load(in, height);
    size = {width, height};
}
