#pragma once

#include "defs.h"

namespace NKikimr {
    namespace NBsController {

        template<typename T>
        struct TResourceValues {
            T DataSize;

            TResourceValues(const TResourceValues&) = default;

            TResourceValues()
                : DataSize(0)
            {}

            TResourceValues(T dataSize)
                : DataSize(dataSize)
            {}

            TResourceValues &operator=(const TResourceValues& other) = default;

            T Max() const {
                return DataSize;
            }

            template<typename TFunc>
            static TResourceValues Apply(const TResourceValues& x, const TResourceValues& y, TFunc&& op) {
                return {
                    op(x.DataSize, y.DataSize),
                };
            }

            template<typename TMultiplier>
            friend TResourceValues operator *(const TResourceValues& x, const TMultiplier& y) {
                return {
                    T(x.DataSize * y),
                };
            }

            friend TResourceValues operator -(const TResourceValues& x, const TResourceValues& y) {
                return Apply(x, y, [](auto x, auto y) { return x - y; });
            }

            friend TResourceValues operator +(const TResourceValues& x, const TResourceValues& y) {
                return Apply(x, y, [](auto x, auto y) { return x + y; });
            }

            TResourceValues& operator +=(const TResourceValues& x) {
                return *this = *this + x;
            }

            TResourceValues<double> Normalize(const TResourceValues& max) const {
                return {
                    max.DataSize ? static_cast<double>(DataSize) / max.DataSize : 0,
                };
            }

            static TResourceValues PiecewiseMax(const TResourceValues& x, const TResourceValues& y) {
                return Apply(x, y, [](auto x, auto y) { return std::max(x, y); });
            }

            TString ToString() const {
                return TStringBuilder() << "(" << DataSize << ")";
            }
        };

        using TResourceRawValues = TResourceValues<i64>;
        using TResourceNormalizedValues = TResourceValues<double>;

    } // NBsController
} // NKikimr
