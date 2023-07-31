#pragma once

#include "common.h"
#include "location.h"

namespace NReverseGeocoder {
    struct Y_PACKED TPoint {
        TCoordinate X;
        TCoordinate Y;

        TPoint()
            : X(0)
            , Y(0)
        {
        }

        TPoint(const TCoordinate& x1, const TCoordinate& y1)
            : X(x1)
            , Y(y1)
        {
        }

        explicit TPoint(const TLocation& l)
            : X(ToCoordinate(l.Lon))
            , Y(ToCoordinate(l.Lat))
        {
        }

        TPoint operator-(const TPoint& p) const {
            return TPoint(X - p.X, Y - p.Y);
        }

        bool operator==(const TPoint& b) const {
            return X == b.X && Y == b.Y;
        }

        bool operator!=(const TPoint& b) const {
            return X != b.X || Y != b.Y;
        }

        bool operator<(const TPoint& b) const {
            return X < b.X || (X == b.X && Y < b.Y);
        }

        TSquare Cross(const TPoint& p) const {
            return 1ll * X * p.Y - 1ll * Y * p.X;
        }
    };

    static_assert(sizeof(TPoint) == 8, "NReverseGeocoder::TPoint size mismatch");

}
