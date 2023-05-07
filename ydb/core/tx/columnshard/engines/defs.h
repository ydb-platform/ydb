#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/tx/ctor_logger.h>

namespace NKikimr::NOlap {

using TLogThis = TCtorLogger<NKikimrServices::TX_COLUMNSHARD>;

enum class TWriteId : ui64 {};

inline TWriteId operator ++(TWriteId& w) noexcept { w = TWriteId{ui64(w) + 1}; return w; }

struct TSnapshot {
    ui64 PlanStep{0};
    ui64 TxId{0};

    ui64 GetPlanStep() const {
        return PlanStep;
    }

    ui64 GetTxId() const {
        return TxId;
    }

    TSnapshot& SetPlanStep(const ui64 value) {
        PlanStep = value;
        return *this;
    }

    TSnapshot& SetTxId(const ui64 value) {
        TxId = value;
        return *this;
    }

    constexpr bool IsZero() const noexcept {
        return PlanStep == 0 && TxId == 0;
    }

    constexpr bool Valid() const noexcept {
        return PlanStep && TxId;
    }

    constexpr auto operator <=> (const TSnapshot&) const noexcept = default;

    static constexpr TSnapshot Max() noexcept {
        return TSnapshot().SetPlanStep(-1ll).SetTxId(-1ll);
    }

    friend IOutputStream& operator << (IOutputStream& out, const TSnapshot& s) {
        return out << "{" << s.PlanStep << "," << s.TxId << "}";
    }
};

inline constexpr bool SnapLess(ui64 planStep1, ui64 txId1, ui64 planStep2, ui64 txId2) noexcept {
    return std::less<TSnapshot>()(TSnapshot{planStep1, txId1}, TSnapshot{planStep2, txId2});
}

inline constexpr bool SnapLessOrEqual(ui64 planStep1, ui64 txId1, ui64 planStep2, ui64 txId2) noexcept {
    return std::less_equal<TSnapshot>()(TSnapshot{planStep1, txId1}, TSnapshot{planStep2, txId2});
}


class IBlobGroupSelector {
protected:
    virtual ~IBlobGroupSelector() = default;
public:
    virtual ui32 GetGroup(const TLogoBlobID& blobId) const = 0;
};

} // namespace NKikimr::NOlap

template<>
struct THash<NKikimr::NOlap::TWriteId> {
    inline size_t operator()(const NKikimr::NOlap::TWriteId x) const noexcept {
        return THash<ui64>()(ui64(x));
    }
};
