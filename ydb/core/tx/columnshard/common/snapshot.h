#pragma once
#include <ydb/library/conclusion/status.h>

#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/datetime/base.h>

namespace NKikimrColumnShardProto {
class TSnapshot;
}

namespace NJson {
class TJsonValue;
};

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

    constexpr TInstant GetPlanInstant() const noexcept {
        return TInstant::MilliSeconds(PlanStep);
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

    static TSnapshot MaxForPlanInstant(const TInstant planInstant) noexcept;

    static TSnapshot MaxForPlanStep(const ui64 planStep) noexcept;

    constexpr bool operator==(const TSnapshot&) const noexcept = default;

    constexpr auto operator<=>(const TSnapshot&) const noexcept = default;

    friend IOutputStream& operator<<(IOutputStream& out, const TSnapshot& s) {
        return out << "{" << s.PlanStep << ':' << (s.TxId == std::numeric_limits<ui64>::max() ? "max" : ::ToString(s.TxId)) << "}";
    }

    template <class TProto>
    void SerializeToProto(TProto& result) const {
        result.SetPlanStep(PlanStep);
        result.SetTxId(TxId);
    }

    NKikimrColumnShardProto::TSnapshot SerializeToProto() const;

    template <class TProto>
    TConclusionStatus DeserializeFromProto(const TProto& proto) {
        PlanStep = proto.GetPlanStep();
        TxId = proto.GetTxId();
        if (!PlanStep) {
            return TConclusionStatus::Fail("incorrect planStep in proto for snapshot");
        }
        if (!TxId) {
            return TConclusionStatus::Fail("incorrect txId in proto for snapshot");
        }
        return TConclusionStatus::Success();
    }

    TConclusionStatus DeserializeFromString(const TString& data);

    TString SerializeToString() const;

    TString DebugString() const;
    NJson::TJsonValue DebugJson() const;
};

} // namespace NKikimr::NOlap
