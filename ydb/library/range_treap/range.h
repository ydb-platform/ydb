#pragma once

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NRangeTreap {

template <class TKey>
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

template <class TKey, class TKeyView>
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

    TOwnedRange<TKey> ToOwnedRange() const {
        TKey leftKey(LeftKey);
        TKey rightKey;
        if (LeftKey.data() != RightKey.data() || LeftKey.size() != RightKey.size()) {
            rightKey = TOwnedCellVec(RightKey);
        } else {
            rightKey = leftKey;
        }
        return TOwnedRange(std::move(leftKey), LeftInclusive, std::move(rightKey), RightInclusive);
    }
};

enum EBorderMode: ui8 {
    LeftExclusive = 0,
    LeftInclusive = 1,
    RightExclusive = 2,
    RightInclusive = 3,
};

class TBorderModeTraits {
public:
    static bool IsInclusive(EBorderMode mode) noexcept {
        return mode & FlagInclusive;
    }

    static bool IsRight(EBorderMode mode) noexcept {
        return mode & FlagRight;
    }

    static std::strong_ordering CompareEqualPoint(const EBorderMode lhs, const EBorderMode rhs) {
        if (lhs == rhs) {
            return std::strong_ordering::equal;
        }
        if (!IsInclusive(lhs)) {
            if (IsRight(lhs)) {
                return std::strong_ordering::greater;
            } else {
                return std::strong_ordering::less;
            }
        }
        if (!IsInclusive(rhs)) {
            if (IsRight(rhs)) {
                return std::strong_ordering::less;
            } else {
                return std::strong_ordering::greater;
            }
        }
        if (IsRight(rhs)) {
            return std::strong_ordering::less;
        } else {
            return std::strong_ordering::greater;
        }
    }

private:
    static const ui8 FlagInclusive = (1 << 0);
    static const ui8 FlagRight = (1 << 1);
};

template <class TKeyView>
class TBorder {
private:
    TKeyView Key;
    YDB_READONLY_DEF(EBorderMode, Mode);

public:
    static TBorder MakeLeft(TKeyView key, bool inclusive) noexcept {
        return { key, inclusive ? EBorderMode::LeftInclusive : EBorderMode::LeftExclusive };
    }

    static TBorder MakeRight(TKeyView key, bool inclusive) noexcept {
        return { key, inclusive ? EBorderMode::RightInclusive : EBorderMode::RightExclusive };
    }

    const TKeyView& GetKey() const {
        return Key;
    }

    TBorder(const TKeyView& key, const EBorderMode mode)
        : Key(key)
        , Mode(mode)
    {
    }
};

template <class TKeyView>
class TDefaultBorderComparator {
    using TBorder = TBorder<TKeyView>;

public:
    static std::strong_ordering Compare(const TBorder& lhs, const TBorder& rhs) {
        if (lhs.GetKey() < rhs.GetKey()) {
            return std::strong_ordering::less;
        }
        if (lhs.GetKey() > rhs.GetKey()) {
            return std::strong_ordering::greater;
        }
        return TBorderModeTraits::CompareEqualPoint(lhs.GetMode(), rhs.GetMode());
    }
};

}   // namespace NKikimr::NRangeTreap
