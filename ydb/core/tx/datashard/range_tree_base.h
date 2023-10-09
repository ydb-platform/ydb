#pragma once

#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NKikimr {
namespace NDataShard {

    class TRangeTreeBase {
    public:
        struct TOwnedRange {
            TOwnedCellVec LeftKey;
            TOwnedCellVec RightKey;
            bool LeftInclusive;
            bool RightInclusive;

            TOwnedRange(TOwnedCellVec leftKey, bool leftInclusive,
                        TOwnedCellVec rightKey, bool rightInclusive) noexcept
                : LeftKey(std::move(leftKey))
                , RightKey(std::move(rightKey))
                , LeftInclusive(leftInclusive)
                , RightInclusive(rightInclusive)
            { }
        };

        struct TRange {
            TConstArrayRef<TCell> LeftKey;
            TConstArrayRef<TCell> RightKey;
            bool LeftInclusive;
            bool RightInclusive;

            TRange(TConstArrayRef<TCell> leftKey, bool leftInclusive,
                   TConstArrayRef<TCell> rightKey, bool rightInclusive) noexcept
                : LeftKey(leftKey)
                , RightKey(rightKey)
                , LeftInclusive(leftInclusive)
                , RightInclusive(rightInclusive)
            { }

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
            Stats_ = { };
        }

    public:
        void SetKeyTypes(const TVector<NScheme::TTypeInfo>& keyTypes) {
            Y_ABORT_UNLESS(keyTypes.size() >= KeyTypes.size(), "Number of key columns must not decrease over time");
            KeyTypes = keyTypes;
        }

        size_t KeyColumns() const noexcept {
            return KeyTypes.size();
        }

    protected:
        struct TBorder {
            TConstArrayRef<TCell> Key;
            EPrefixMode Mode;

            bool IsInclusive() const noexcept {
                return IsInclusive(Mode);
            }

            static bool IsInclusive(EPrefixMode mode) noexcept {
                return (mode & PrefixModeOrderMask) == PrefixModeOrderCur;
            }

            static TBorder MakeLeft(TConstArrayRef<TCell> key, bool inclusive) noexcept {
                return { key, inclusive ? PrefixModeLeftBorderInclusive : PrefixModeLeftBorderNonInclusive };
            }

            static TBorder MakeRight(TConstArrayRef<TCell> key, bool inclusive) noexcept {
                return { key, inclusive ? PrefixModeRightBorderInclusive : PrefixModeRightBorderNonInclusive };
            }
        };

        int CompareBorders(const TBorder& a, const TBorder& b) const noexcept {
            ++Stats_.Comparisons;
            return ComparePrefixBorders(KeyTypes, a.Key, a.Mode, b.Key, b.Mode);
        }

    protected:
        size_t Size_ = 0;
        mutable TStats Stats_;
        TVector<NScheme::TTypeInfo> KeyTypes;
    };

} // namespace NDataShard
} // namespace NKikimr
