#pragma once

#include "common.h"
#include "point.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NReverseGeocoder {
    // TEdge is a type, which represent polygon edge, Beg/End refers on begin/End edge points in
    // geographical data.
    struct Y_PACKED TEdge {
        TRef Beg;
        TRef End;

        TEdge()
            : Beg(0)
            , End(0)
        {
        }

        TEdge(const TRef& a, const TRef& b)
            : Beg(a)
            , End(b)
        {
        }

        bool operator==(const TEdge& e) const {
            return Beg == e.Beg && End == e.End;
        }

        bool operator!=(const TEdge& e) const {
            return Beg != e.Beg || End != e.End;
        }

        bool operator<(const TEdge& e) const {
            return Beg < e.Beg || (Beg == e.Beg && End < e.End);
        }

        // Checks that current edge is lying lower then other edge. Both edges must have a common X
        // values, otherwise the behavior is undefined.
        bool Lower(const TEdge& e, const TPoint* points) const {
            if (*this == e)
                return false;

            const TPoint& a1 = points[Beg];
            const TPoint& a2 = points[End];
            const TPoint& b1 = points[e.Beg];
            const TPoint& b2 = points[e.End];

            Y_ASSERT(a1.X <= a2.X && b1.X <= b2.X);

            if (a1 == b1) {
                return (a2 - a1).Cross(b2 - a1) > 0;
            } else if (a2 == b2) {
                return (a1 - b1).Cross(b2 - b1) > 0;
            } else if (b1.X >= a1.X && b1.X <= a2.X) {
                return (a2 - a1).Cross(b1 - a1) > 0;
            } else if (b2.X >= a1.X && b2.X <= a2.X) {
                return (a2 - a1).Cross(b2 - a1) > 0;
            } else if (a1.X >= b1.X && a1.X <= b2.X) {
                return (a1 - b1).Cross(b2 - b1) > 0;
            } else if (a2.X >= b1.X && a2.X <= b2.X) {
                return (a2 - b1).Cross(b2 - b1) > 0;
            } else {
                return false;
            }
        }

        // Checks that current edge lying lower then given point. Edge and point must have a common X
        // values, otherwise the behavior is undefined.
        bool Lower(const TPoint& p, const TPoint* points) const {
            if (Contains(p, points))
                return false;

            TPoint a = points[Beg];
            TPoint b = points[End];

            if (a.X > b.X)
                DoSwap(a, b);

            return (b - a).Cross(p - a) > 0;
        }

        bool Contains(const TPoint& p, const TPoint* points) const {
            TPoint a = points[Beg];
            TPoint b = points[End];

            if (a.X > b.X)
                DoSwap(a, b);

            if (p.X < a.X || p.X > b.X)
                return false;

            return (b - a).Cross(p - a) == 0;
        }
    };

    static_assert(sizeof(TEdge) == 8, "NReverseGeocoder::TEdge size mismatch");

}
