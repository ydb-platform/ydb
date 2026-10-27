#include "flat_tier_policy.h"

namespace NKikimr {
namespace NTable {

namespace {

    void BoundToProto(const TTierBound& bound, NTabletFlatScheme::TTierBoundSettings& proto) {
        if (const auto* relative = std::get_if<TTierRelativeBound>(&bound)) {
            proto.SetRelativeOffsetUs(relative->Offset.MicroSeconds());
        } else {
            proto.SetAbsoluteMicros(std::get<TTierAbsoluteBound>(bound).Micros);
        }
    }

    TTierBound BoundFromProto(const NTabletFlatScheme::TTierBoundSettings& proto) {
        switch (proto.GetKindCase()) {
            case NTabletFlatScheme::TTierBoundSettings::kRelativeOffsetUs:
                return TTierRelativeBound{ TDuration::MicroSeconds(proto.GetRelativeOffsetUs()) };
            case NTabletFlatScheme::TTierBoundSettings::kAbsoluteMicros:
                return TTierAbsoluteBound{ proto.GetAbsoluteMicros() };
            default:
                Y_ENSURE(false, "Tier bound has no value");
        }
    }

} // namespace

    void TieringPolicyToProto(const TTieringPolicy& policy, NTabletFlatScheme::TTieringPolicySettings& proto) {
        proto.Clear();
        proto.SetColumnTag(policy.ColumnTag);
        proto.SetUnit(policy.Unit);
        for (const auto& rule : policy.Rules) {
            auto* ruleProto = proto.AddRules();
            ruleProto->SetTierId(rule.TierId);
            for (const auto& range : rule.Ranges) {
                auto* rangeProto = ruleProto->AddRanges();
                if (range.From) {
                    BoundToProto(*range.From, *rangeProto->MutableFrom());
                }
                if (range.To) {
                    BoundToProto(*range.To, *rangeProto->MutableTo());
                }
                rangeProto->SetToInclusive(range.ToInclusive);
            }
        }
    }

    TTieringPolicy TieringPolicyFromProto(const NTabletFlatScheme::TTieringPolicySettings& proto) {
        TTieringPolicy policy;
        policy.ColumnTag = proto.GetColumnTag();
        policy.Unit = proto.GetUnit();
        for (const auto& ruleProto : proto.GetRules()) {
            auto& rule = policy.Rules.emplace_back();
            rule.TierId = ruleProto.GetTierId();
            for (const auto& rangeProto : ruleProto.GetRanges()) {
                auto& range = rule.Ranges.emplace_back();
                if (rangeProto.HasFrom()) {
                    range.From = BoundFromProto(rangeProto.GetFrom());
                }
                if (rangeProto.HasTo()) {
                    range.To = BoundFromProto(rangeProto.GetTo());
                }
                range.ToInclusive = rangeProto.GetToInclusive();
            }
        }
        return policy;
    }

}
}
