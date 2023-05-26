#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/ctor_logger.h>

namespace NKikimr::NOlap {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

enum class TWriteId : ui64 {};

inline TWriteId operator++(TWriteId& w) noexcept {
    w = TWriteId{ui64(w) + 1};
    return w;
}

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
        return out << "{" << s.PlanStep << ':' << (s.TxId == std::numeric_limits<ui64>::max() ? "max" : ToString(s.TxId)) << "}";
    }
};

class IBlobGroupSelector {
protected:
    virtual ~IBlobGroupSelector() = default;

public:
    virtual ui32 GetGroup(const TLogoBlobID& blobId) const = 0;
};

} // namespace NKikimr::NOlap

template <>
struct THash<NKikimr::NOlap::TWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};
