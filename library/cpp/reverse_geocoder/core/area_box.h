#pragma once

#include "common.h"
#include "point.h"

namespace NReverseGeocoder {
    namespace NAreaBox {
        const TCoordinate LowerX = ToCoordinate(-180.0);
        const TCoordinate UpperX = ToCoordinate(180.0);
        const TCoordinate LowerY = ToCoordinate(-90.0);
        const TCoordinate UpperY = ToCoordinate(90.0);
        const TCoordinate DeltaX = ToCoordinate(0.1);
        const TCoordinate DeltaY = ToCoordinate(0.1);
        const TCoordinate NumberX = (UpperX - LowerX) / DeltaX;
        const TCoordinate NumberY = (UpperY - LowerY) / DeltaY;
        const TCoordinate Number = NumberX * NumberY;

    }

    // Area of geo territory. Variable PolygonRefsOffset refers to the polygons lying inside this
    // area. Geo map is divided into equal bounding boxes from (NAreaBox::LowerX, NAreaBox::LowerY)
    // to (NAreaBox::UpperX, NAreaBox::UpperY) with DeltaX and DeltaY sizes. Logic of filling is in
    // generator.
    struct Y_PACKED TAreaBox {
        TNumber PolygonRefsOffset;
        TNumber PolygonRefsNumber;
    };

    static_assert(sizeof(TAreaBox) == 8, "NReverseGeocoder::TAreaBox size mismatch");

    // Determine in wich area box in geoData is point.
    TRef LookupAreaBox(const TPoint& point);

}
