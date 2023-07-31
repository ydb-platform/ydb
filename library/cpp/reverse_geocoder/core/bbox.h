#pragma once

#include "common.h"
#include "point.h"

#include <util/generic/utility.h>

namespace NReverseGeocoder {
    struct Y_PACKED TBoundingBox {
        TCoordinate X1;
        TCoordinate Y1;
        TCoordinate X2;
        TCoordinate Y2;

        TBoundingBox()
            : X1(0)
            , Y1(0)
            , X2(0)
            , Y2(0)
        {
        }

        TBoundingBox(TCoordinate x1, TCoordinate y1, TCoordinate x2, TCoordinate y2)
            : X1(x1)
            , Y1(y1)
            , X2(x2)
            , Y2(y2)
        {
        }

        TBoundingBox(const TPoint* points, TNumber number) {
            Init();
            for (TNumber i = 0; i < number; ++i)
                Relax(points[i]);
        }

        void Init() {
            X1 = ToCoordinate(180.0);
            Y1 = ToCoordinate(90.0);
            X2 = ToCoordinate(-180.0);
            Y2 = ToCoordinate(-90.0);
        }

        void Relax(const TPoint& p) {
            X1 = Min(X1, p.X);
            Y1 = Min(Y1, p.Y);
            X2 = Max(X2, p.X);
            Y2 = Max(Y2, p.Y);
        }

        bool HasIntersection(const TBoundingBox& r) const {
            if (X1 > r.X2 || X2 < r.X1 || Y1 > r.Y2 || Y2 < r.Y1)
                return false;
            return true;
        }

        bool Contains(const TPoint& p) const {
            if (p.X < X1 || p.X > X2 || p.Y < Y1 || p.Y > Y2)
                return false;
            return true;
        }
    };

    static_assert(sizeof(TBoundingBox) == 16, "NReverseGeocoder::TBoundingBox size mismatch");

}
