#pragma once

#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NIntervalTree {

    template <class TKey, class TKeyView>
    class TRangeTreeBase {
    public:
        struct TOwnedRange {
            TKey LeftKey;
            TKey RightKey;
            bool LeftInclusive;
            bool RightInclusive;

            TOwnedRange(TKey leftKey, bool leftInclusive, TKey rightKey, bool rightInclusive) noexcept
                : LeftKey(std::move(leftKey))
                , RightKey(std::move(rightKey))
                , LeftInclusive(leftInclusive)
                , RightInclusive(rightInclusive)
            {
            }
        };

        struct TRange {
            TKeyView LeftKey;
            TKeyView RightKey;
            bool LeftInclusive;
            bool RightInclusive;

            TRange(TKeyView leftKey, bool leftInclusive, TKeyView rightKey, bool rightInclusive) noexcept
                : LeftKey(leftKey)
                , RightKey(rightKey)
                , LeftInclusive(leftInclusive)
                , RightInclusive(rightInclusive)
            {
            }

            TOwnedRange ToOwnedRange() const {
                TOwnedCellVec leftKey(LeftKey);
                TOwnedCellVec rightKey;
                if (LeftKey.data() != RightKey.data() || LeftKey.size() != RightKey.size()) {
                    rightKey = TOwnedCellVec(RightKey);
                } else {
                    rightKey = leftKey;
                }
                return TOwnedRange(std::move(leftKey), LeftInclusive, std::move(rightKey), RightInclusive);
            }
        };

        struct TStats {
            size_t Comparisons = 0;
            size_t Inserts = 0;
            size_t Updates = 0;
            size_t Deletes = 0;
        };

    public:
        size_t Size() const noexcept {
            return Size_;
        }

        const TStats& Stats() const noexcept {
            return Stats_;
        }

        void ResetStats() noexcept {
            Stats_ = {};
        }

    public:
        struct TBorder {
            TKeyView Key;
            EPrefixMode Mode;

            bool IsInclusive() const noexcept {
                return IsInclusive(Mode);
            }

            static bool IsInclusive(EPrefixMode mode) noexcept {
                return (mode & PrefixModeOrderMask) == PrefixModeOrderCur;
            }

            static TBorder MakeLeft(TKeyView key, bool inclusive) noexcept {
                return { key, inclusive ? PrefixModeLeftBorderInclusive : PrefixModeLeftBorderNonInclusive };
            }

            static TBorder MakeRight(TKeyView key, bool inclusive) noexcept {
                return { key, inclusive ? PrefixModeRightBorderInclusive : PrefixModeRightBorderNonInclusive };
            }
        };

    protected:
        size_t Size_ = 0;
        mutable TStats Stats_;
    };

}   // namespace NIntervalTree
} // namespace NKikimr
