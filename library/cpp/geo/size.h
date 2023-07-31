#pragma once

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NGeo {
    class TSize {
    public:
        TSize(double width, double height) noexcept
            : Width_(width)
            , Height_(height)
        {
        }

        explicit TSize(double size) noexcept
            : Width_(size)
            , Height_(size)
        {
        }

        TSize() noexcept
            : Width_(BadWidth)
            , Height_(BadHeight)
        {
        }

        double GetWidth() const noexcept {
            return Width_;
        }

        double GetHeight() const noexcept {
            return Height_;
        }

        void swap(TSize& s) noexcept {
            std::swap(Width_, s.Width_);
            std::swap(Height_, s.Height_);
        }

        bool IsValid() const {
            return (Width_ != BadWidth) && (Height_ != BadHeight);
        }

        void Stretch(double multiplier) {
            Width_ *= multiplier;
            Height_ *= multiplier;
        }

        void Inflate(double additionX, double additionY) {
            Width_ += additionX;
            Height_ += additionY;
        }

        bool operator!() const {
            return !IsValid();
        }

        TString ToCgiStr() const {
            TString s = ToString(Width_);
            s.append(',');
            s.append(ToString(Height_));
            return s;
        }

        /**
         * try to parse TSize
         * return parsed TSize on success, otherwise throw exception
         */
        static TSize Parse(TStringBuf s, TStringBuf delimiter = TStringBuf(","));

        /**
         * try to parse TSize
         * return TMaybe of parsed TSize on success, otherwise return empty TMaybe
         */
        static TMaybe<TSize> TryParse(TStringBuf s, TStringBuf delimiter = TStringBuf(","));

    private:
        double Width_;
        double Height_;
        static const double BadWidth;
        static const double BadHeight;
    };

    inline bool operator==(const TSize& p1, const TSize& p2) {
        return p1.GetHeight() == p2.GetHeight() && p1.GetWidth() == p2.GetWidth();
    }
} // namespace NGeo

template <>
inline void Out<NGeo::TSize>(IOutputStream& o, const NGeo::TSize& s) {
    o << '<' << s.GetWidth() << ", " << s.GetHeight() << '>';
}
