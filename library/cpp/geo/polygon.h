#pragma once

#include "point.h"
#include "window.h"

#include <util/ysaveload.h>
#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <algorithm>
#include <functional>

namespace NGeo {
    class TGeoPolygon {
    private:
        TVector<TGeoPoint> Points_;
        TGeoWindow Window_;

    public:
        TGeoPolygon() = default;

        explicit TGeoPolygon(const TVector<TGeoPoint>& points)
            : Points_(points)
        {
            CalcWindow();
        }

        const TVector<TGeoPoint>& GetPoints() const {
            return Points_;
        }

        const TGeoWindow& GetWindow() const {
            return Window_;
        }

        void swap(TGeoPolygon& o) noexcept {
            Points_.swap(o.Points_);
            Window_.swap(o.Window_);
        }

        bool IsValid() const noexcept {
            return !Points_.empty() && Window_.IsValid();
        }

        bool operator!() const {
            return !IsValid();
        }

        /**
         * try to parse TGeoPolygon from string which stores points
         * coords are separated by llDelimiter, points are separated by pointsDelimiter
         * return parsed TGeoPolygon on success, otherwise throw exception
         */
        static TGeoPolygon Parse(TStringBuf s, TStringBuf llDelimiter = ",", TStringBuf pointsDelimiter = TStringBuf(" "));

        /**
         * try to parse TGeoPolygon from string which stores points
         * coords are separated by llDelimiter, points are separated by pointsDelimiter
         * return TMaybe of parsed TGeoPolygon on success, otherwise return empty TMaybe
         */
        static TMaybe<TGeoPolygon> TryParse(TStringBuf s, TStringBuf llDelimiter = ",", TStringBuf pointsDelimiter = TStringBuf(" "));

    private:
        void CalcWindow() {
            auto getLon = std::mem_fn(&TGeoPoint::Lon);
            double lowerX = MinElementBy(Points_.begin(), Points_.end(), getLon)->Lon();
            double upperX = MaxElementBy(Points_.begin(), Points_.end(), getLon)->Lon();

            auto getLat = std::mem_fn(&TGeoPoint::Lat);
            double lowerY = MinElementBy(Points_.begin(), Points_.end(), getLat)->Lat();
            double upperY = MaxElementBy(Points_.begin(), Points_.end(), getLat)->Lat();

            Window_ = TGeoWindow{TGeoPoint{lowerX, lowerY}, TGeoPoint{upperX, upperY}};
        }
    };

    inline bool operator==(const TGeoPolygon& p1, const TGeoPolygon& p2) {
        return p1.GetPoints() == p2.GetPoints();
    }

    inline bool operator!=(const TGeoPolygon& p1, const TGeoPolygon& p2) {
        return !(p1 == p2);
    }
} // namespace NGeo
