#include "one_layer.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/level/common_level.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TConclusionStatus TOneLayerConstructor::DoDeserializeFromJson(const NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        return TConclusionStatus::Fail("incorrect level description");
    }
    if (json.Has("size_limit_guarantee")) {
        const auto& jsonValue = json["size_limit_guarantee"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect size_limit_guarantee value (have to be unsigned integer)");
        }
        SizeLimitGuarantee = jsonValue.GetUInteger();
    }
    if (json.Has("bytes_limit_fraction")) {
        const auto& jsonValue = json["bytes_limit_fraction"];
        if (!jsonValue.IsDouble()) {
            return TConclusionStatus::Fail("incorrect bytes_limit_fraction value (have to be double)");
        }
        BytesLimitFraction = jsonValue.GetDouble();
        if (BytesLimitFraction < 0 || BytesLimitFraction > 1) {
            return TConclusionStatus::Fail("bytes_limit_fraction have to been in [0, 1]");
        }
    }
    if (json.Has("expected_portion_size")) {
        const auto& jsonValue = json["expected_portion_size"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect expected_portion_size value (have to be unsigned int)");
        }
        ExpectedPortionSize = jsonValue.GetUInteger();
    }
    return TConclusionStatus::Success();
}

bool TOneLayerConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) {
    if (!proto.HasOneLayer()) {
        return true;
    }
    if (proto.GetOneLayer().HasExpectedPortionSize()) {
        ExpectedPortionSize = proto.GetOneLayer().GetExpectedPortionSize();
    }
    if (proto.GetOneLayer().HasBytesLimitFraction()) {
        BytesLimitFraction = proto.GetOneLayer().GetBytesLimitFraction();
    }
    if (proto.GetOneLayer().HasSizeLimitGuarantee()) {
        SizeLimitGuarantee = proto.GetOneLayer().GetSizeLimitGuarantee();
    }
    return true;
}

void TOneLayerConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const {
    if (ExpectedPortionSize) {
        proto.MutableOneLayer()->SetExpectedPortionSize(*ExpectedPortionSize);
    }
    if (BytesLimitFraction) {
        proto.MutableOneLayer()->SetBytesLimitFraction(*BytesLimitFraction);
    }
    if (SizeLimitGuarantee) {
        proto.MutableOneLayer()->SetSizeLimitGuarantee(SizeLimitGuarantee);
    }
}

std::shared_ptr<NStorageOptimizer::NLCBuckets::IPortionsLevel> TOneLayerConstructor::DoBuildLevel(
    const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const std::shared_ptr<TSimplePortionsGroupInfo>& portionsInfo,
    const TLevelCounters& counters, const std::vector<std::shared_ptr<IPortionsSelector>>& selectors) const {
    return std::make_shared<TOneLayerPortions>(indexLevel, BytesLimitFraction.value_or(1), ExpectedPortionSize.value_or(2 << 20), nextLevel,
        portionsInfo, counters, SizeLimitGuarantee, selectors, GetDefaultSelectorName());
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
