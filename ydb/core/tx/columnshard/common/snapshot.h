#pragma once
#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NKikimr::NOlap {

class TSnapshot {
private:
    ui64 PlanStep = 0;
    ui64 TxId = 0;

public:
    constexpr TSnapshot(const ui64 planStep, const ui64 txId) noexcept
        : PlanStep(planStep)
        , TxId(txId) {
    }

    constexpr ui64 GetPlanStep() const noexcept {
        return PlanStep;
    }

    constexpr ui64 GetTxId() const noexcept {
        return TxId;
    }

    constexpr bool IsZero() const noexcept {
        return PlanStep == 0 && TxId == 0;
    }

    constexpr bool Valid() const noexcept {
        return PlanStep && TxId;
    }

    static constexpr TSnapshot Zero() noexcept {
        return TSnapshot(0, 0);
    }

    static constexpr TSnapshot Max() noexcept {
        return TSnapshot(-1ll, -1ll);
    }

    constexpr bool operator==(const TSnapshot&) const noexcept = default;

    constexpr auto operator<=>(const TSnapshot&) const noexcept = default;

    friend IOutputStream& operator<<(IOutputStream& out, const TSnapshot& s) {
        return out << "{" << s.PlanStep << ':' << (s.TxId == std::numeric_limits<ui64>::max() ? "max" : ::ToString(s.TxId)) << "}";
    }

    TString DebugString() const;
};

} // namespace NKikimr::NOlap
