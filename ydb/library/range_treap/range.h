#pragma once

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NRangeTreap {

template <class TKey>
struct TRange {
    TKey LeftKey;
    TKey RightKey;
    bool LeftInclusive;
    bool RightInclusive;

    TRange(TKey leftKey, bool leftInclusive, TKey rightKey, bool rightInclusive) noexcept
        : LeftKey(std::move(leftKey))
        , RightKey(std::move(rightKey))
        , LeftInclusive(leftInclusive)
        , RightInclusive(rightInclusive)
    {
    }
};

enum class EBorderMode: ui8 {
    RightExclusive = 0,
    LeftInclusive = 1,
    RightInclusive = 2,
    LeftExclusive = 3,
};

class TBorderModeTraits {
public:
    static bool IsInclusive(EBorderMode mode) noexcept {
        return mode == EBorderMode::LeftInclusive || mode == EBorderMode::RightInclusive;
    }

    static bool IsRight(EBorderMode mode) noexcept {
        return (ui8)mode & 1 == 0;
    }

    static int CompareEqualPoint(const EBorderMode lhs, const EBorderMode rhs) {
        return (int)lhs - (int)rhs;
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

template <class TKey, class TKeyView>
class TDefaultBorderComparator {
    using TBorder = TBorder<TKeyView>;

public:
    static int Compare(const TBorder& lhs, const TBorder& rhs) {
        if (lhs.GetKey() < rhs.GetKey()) {
            return -1;
        }
        if (lhs.GetKey() > rhs.GetKey()) {
            return 1;
        }
        return TBorderModeTraits::CompareEqualPoint(lhs.GetMode(), rhs.GetMode());
    }

    static void ValidateKey(const TKey& /*key*/) {
        // Do nothing
    }

    static TKey MakeOwnedKey(const TKeyView& keyView) {
        return TKey(keyView);
    }

    static bool IsEqualFast(const TKeyView& lhs, const TKeyView& rhs) {
        return lhs == rhs;
    }
};

}   // namespace NKikimr::NRangeTreap
