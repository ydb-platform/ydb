#pragma once

#include <util/generic/utility.h>

#include "point.h"

namespace NGeo {

    class TGeoBoundingBox {
    public:
        TGeoBoundingBox()

            = default;

        TGeoBoundingBox(const TGeoPoint& p1, const TGeoPoint& p2) {
            MinX_ = Min(p1.Lon(), p2.Lon());
            MaxX_ = Max(p1.Lon(), p2.Lon());
            MinY_ = Min(p1.Lat(), p2.Lat());
            MaxY_ = Max(p1.Lat(), p2.Lat());
        }

        const double& GetMinX() const {
            return MinX_;
        }

        const double& GetMaxX() const {
            return MaxX_;
        }

        const double& GetMinY() const {
            return MinY_;
        }

        const double& GetMaxY() const {
            return MaxY_;
        }

        double Width() const {
            return MaxX_ - MinX_;
        }

        double Height() const {
            return MaxY_ - MinY_;
        }

    private:
        double MinX_{std::numeric_limits<double>::quiet_NaN()};
        double MaxX_{std::numeric_limits<double>::quiet_NaN()};
        double MinY_{std::numeric_limits<double>::quiet_NaN()};
        double MaxY_{std::numeric_limits<double>::quiet_NaN()};
    };

    inline bool operator==(const TGeoBoundingBox& a, const TGeoBoundingBox& b) {
        return a.GetMinX() == b.GetMinX() &&
               a.GetMinY() == b.GetMinY() &&
               a.GetMaxX() == b.GetMaxX() &&
               a.GetMaxY() == b.GetMaxY();
    }
} // namespace NGeo
