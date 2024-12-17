#include "zero_level.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/zero_level.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TConclusionStatus TZeroLevelConstructor::DoDeserializeFromJson(const NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        return TConclusionStatus::Fail("incorrect level description");
    }
    if (json.Has("portions_live_duration")) {
        const auto& jsonValue = json["portions_live_duration"];
        if (!jsonValue.IsString()) {
            return TConclusionStatus::Fail("incorrect portions_live_duration value (have to be similar as 10s, 20m, 30d, etc)");
        }
        TDuration d;
        if (!TDuration::TryParse(jsonValue.GetString(), d)) {
            return TConclusionStatus::Fail("cannot parse portions_live_duration value " + jsonValue.GetString());
        }
        PortionsLiveDuration = d;
    }
    if (json.Has("expected_blobs_size")) {
        const auto& jsonValue = json["expected_blobs_size"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect expected_blobs_size value (have to be unsigned int)");
        }
        ExpectedBlobsSize = jsonValue.GetUInteger();
    }
    return TConclusionStatus::Success();
}

bool TZeroLevelConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) {
    if (!proto.HasZeroLevel()) {
        return true;
    }
    if (proto.GetZeroLevel().HasPortionsLiveDurationSeconds()) {
        PortionsLiveDuration = TDuration::Seconds(proto.GetZeroLevel().GetPortionsLiveDurationSeconds());
    }
    if (proto.GetZeroLevel().HasExpectedBlobsSize()) {
        ExpectedBlobsSize = proto.GetZeroLevel().GetExpectedBlobsSize();
    }
    return true;
}

void TZeroLevelConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const {
    if (PortionsLiveDuration) {
        proto.MutableZeroLevel()->SetPortionsLiveDurationSeconds(PortionsLiveDuration->Seconds());
    }
    if (ExpectedBlobsSize) {
        proto.MutableZeroLevel()->SetExpectedBlobsSize(*ExpectedBlobsSize);
    }
}

std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::NLCBuckets::IPortionsLevel> TZeroLevelConstructor::DoBuildLevel(
    const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const TLevelCounters& counters) const {
    return std::make_shared<TZeroLevelPortions>(
        indexLevel, nextLevel, counters, PortionsLiveDuration.value_or(TDuration::Max()), ExpectedBlobsSize.value_or((ui64)1 << 20));
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
