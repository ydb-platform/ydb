#pragma once

#include "point.h"
#include "size.h"
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/generic/maybe.h>

#include <algorithm>

namespace NGeo {
    class TGeoWindow {
    public:
        TGeoWindow() noexcept

            = default;

        TGeoWindow(const TGeoPoint& center, const TSize& size) noexcept
            : Center_(center)
            , Size_(size)
        {
            CalcCorners();
        }

        TGeoWindow(const TGeoPoint& firstPoint, const TGeoPoint& secondPoint) noexcept
            : LowerLeftCorner_{std::min(firstPoint.Lon(), secondPoint.Lon()),
                               std::min(firstPoint.Lat(), secondPoint.Lat())}
            , UpperRightCorner_{std::max(firstPoint.Lon(), secondPoint.Lon()),
                                std::max(firstPoint.Lat(), secondPoint.Lat())}
        {
            CalcCenterAndSpan();
        }

        const TGeoPoint& GetCenter() const noexcept {
            return Center_;
        }

        void SetCenter(const TGeoPoint& newCenter) {
            Center_ = newCenter;
            CalcCorners();
        }

        const TSize& GetSize() const noexcept {
            return Size_;
        }

        void SetSize(const TSize& newSize) {
            Size_ = newSize;
            CalcCorners();
        }

        const TGeoPoint& GetLowerLeftCorner() const noexcept {
            return LowerLeftCorner_;
        }

        const TGeoPoint& GetUpperRightCorner() const noexcept {
            return UpperRightCorner_;
        }

        void swap(TGeoWindow& o) noexcept {
            Center_.swap(o.Center_);
            Size_.swap(o.Size_);
            LowerLeftCorner_.swap(o.LowerLeftCorner_);
            UpperRightCorner_.swap(o.UpperRightCorner_);
        }

        bool IsValid() const noexcept {
            return Center_.IsValid() && Size_.IsValid();
        }

        bool Contains(const TGeoPoint&) const;

        bool Contains(const TGeoWindow& w) const {
            return Contains(w.LowerLeftCorner_) && Contains(w.UpperRightCorner_);
        }

        void Stretch(double multiplier) {
            Size_.Stretch(multiplier);
            CalcCorners();
        }

        void Inflate(double additionX, double additionY) {
            Size_.Inflate(additionX * 2, additionY * 2);
            CalcCorners();
        }

        void Inflate(double addition) {
            Inflate(addition, addition);
        }

        bool operator!() const {
            return !IsValid();
        }

        double Diameter() const;

        double Area() const {
            return Size_.GetHeight() * Size_.GetWidth();
        }

        double Distance(const TGeoWindow&) const;

        double GetApproxDistance(const TPointLL& point) const;

        /**
         * try to parse TGeoWindow from center and span
         * return parsed TGeoWindow on success, otherwise throw exception
         */
        static TGeoWindow ParseFromLlAndSpn(TStringBuf llStr, TStringBuf spnStr, TStringBuf delimiter = TStringBuf(","));

        /**
         * try to parse TGeoWindow from two corners
         * return parsed TGeoWindow on success, otherwise throw exception
         */
        static TGeoWindow ParseFromCornersPoints(TStringBuf leftCornerStr, TStringBuf rightCornerStr, TStringBuf delimiter = TStringBuf(","));

        /**
         * try to parse TGeoWindow from center and span
         * return TMaybe of parsed TGeoWindow on success, otherwise return empty TMaybe
         */
        static TMaybe<TGeoWindow> TryParseFromLlAndSpn(TStringBuf llStr, TStringBuf spnStr, TStringBuf delimiter = TStringBuf(","));

        /**
         * try to parse TGeoWindow from two corners
         * return TMaybe of parsed TGeoWindow on success, otherwise return empty TMaybe
         */
        static TMaybe<TGeoWindow> TryParseFromCornersPoints(TStringBuf leftCornerStr, TStringBuf rightCornerStr, TStringBuf delimiter = TStringBuf(","));

    private:
        TGeoPoint Center_;
        TSize Size_;
        TGeoPoint LowerLeftCorner_;
        TGeoPoint UpperRightCorner_;

        void CalcCorners();
        void CalcCenterAndSpan();
    };

    inline bool operator==(const TGeoWindow& lhs, const TGeoWindow& rhs) {
        return lhs.GetCenter() == rhs.GetCenter() && lhs.GetSize() == rhs.GetSize();
    }

    inline bool operator!=(const TGeoWindow& p1, const TGeoWindow& p2) {
        return !(p1 == p2);
    }

    /**
     * \class TMercatorWindow
     *
     * Represents a window in EPSG:3395 projection
     * (WGS 84 / World Mercator)
     */
    class TMercatorWindow {
    public:
        TMercatorWindow() noexcept;
        TMercatorWindow(const TMercatorPoint& center, const TSize& size) noexcept;
        TMercatorWindow(const TMercatorPoint& firstPoint, const TMercatorPoint& secondPoint) noexcept;

        const TMercatorPoint& GetCenter() const noexcept {
            return Center_;
        }

        TSize GetHalfSize() const noexcept {
            return {HalfWidth_, HalfHeight_};
        }

        TSize GetSize() const noexcept {
            return {GetWidth(), GetHeight()};
        }

        double GetWidth() const noexcept {
            return HalfWidth_ * 2;
        }

        double GetHeight() const noexcept {
            return HalfHeight_ * 2;
        }

        TMercatorPoint GetLowerLeftCorner() const noexcept {
            return TMercatorPoint{Center_.X() - HalfWidth_, Center_.Y() - HalfHeight_};
        }

        TMercatorPoint GetUpperRightCorner() const noexcept {
            return TMercatorPoint{Center_.X() + HalfWidth_, Center_.Y() + HalfHeight_};
        }

        bool Contains(const TMercatorPoint& pt) const noexcept;

        bool Contains(const TMercatorWindow& w) const {
            return Contains(w.GetLowerLeftCorner()) && Contains(w.GetUpperRightCorner());
        }

        void Stretch(double multiplier) {
            HalfWidth_ *= multiplier;
            HalfHeight_ *= multiplier;
        }

        void Inflate(double additionX, double additionY) {
            HalfWidth_ += additionX;
            HalfHeight_ += additionY;
        }

        void Inflate(double addition) {
            Inflate(addition, addition);
        }

        double Area() const {
            return GetHeight() * GetWidth();
        }

    private:
        bool IsDefined() const {
            return Center_.IsDefined() && !std::isnan(HalfWidth_) && !std::isnan(HalfHeight_);
        }

    private:
        TMercatorPoint Center_;
        double HalfWidth_;
        double HalfHeight_;
    };

    inline bool operator==(const TMercatorWindow& lhs, const TMercatorWindow& rhs) {
        return lhs.GetCenter() == rhs.GetCenter() && lhs.GetHalfSize() == rhs.GetHalfSize();
    }

    inline bool operator!=(const TMercatorWindow& p1, const TMercatorWindow& p2) {
        return !(p1 == p2);
    }

    /**
     * Typedefs
     * TODO(sobols@): remove
     */

    using TWindowLL = TGeoWindow;

    /**
     * Conversion
     */

    TMercatorWindow LLToMercator(const TGeoWindow&);
    TGeoWindow MercatorToLL(const TMercatorWindow&);

    /**
     * Utility functions
     */

    bool Contains(const TMaybe<TGeoWindow>& window, const TGeoPoint& point);

    TMaybe<TGeoWindow> Union(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs);
    TGeoWindow Union(const TGeoWindow& lhs, const TGeoWindow& rhs);

    TMaybe<TGeoWindow> Intersection(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs);
    TMaybe<TGeoWindow> Intersection(const TGeoWindow& lhs, const TGeoWindow& rhs);

    bool Intersects(const TGeoWindow& lhs, const TGeoWindow& rhs);
    bool Intersects(const TMaybe<TGeoWindow>& lhs, const TMaybe<TGeoWindow>& rhs);
} // namespace NGeo

template <>
inline void Out<NGeo::TGeoWindow>(IOutputStream& o, const NGeo::TGeoWindow& obj) {
    o << '{' << obj.GetCenter() << ", " << obj.GetSize() << ", " << obj.GetLowerLeftCorner() << ", " << obj.GetUpperRightCorner() << "}";
}
