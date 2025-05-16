#include "zero_level.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/zero_level.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

TConclusionStatus TZeroLevelConstructor::DoDeserializeFromJson(const NJson::TJsonValue& json) {
    if (!json.IsMap()) {
        return TConclusionStatus::Fail("incorrect level description");
    }
    if (json.Has("data_snapshot_interval")) {
        const auto& jsonValue = json["data_snapshot_interval"];
        if (!jsonValue.IsMap()) {
            return TConclusionStatus::Fail("incorrect data_snapshot_interval value (have to be map)");
        }
        auto conclusion = DataSnapshotInterval.DeserializeFromJson(jsonValue);
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    if (json.Has("portions_count_available")) {
        const auto& jsonValue = json["portions_count_available"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect portions_count_available value (have to be unsigned int)");
        }
        PortionsCountAvailable = jsonValue.GetUInteger();
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
    if (json.Has("portions_count_limit")) {
        const auto& jsonValue = json["portions_count_limit"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect portions_count_limit value (have to be unsigned int)");
        }
        PortionsCountLimit = jsonValue.GetUInteger();
    }
    if (json.Has("portions_size_limit")) {
        const auto& jsonValue = json["portions_size_limit"];
        if (!jsonValue.IsUInteger()) {
            return TConclusionStatus::Fail("incorrect portions_size_limit value (have to be unsigned int)");
        }
        PortionsSizeLimit = jsonValue.GetUInteger();
    }
    return TConclusionStatus::Success();
}

bool TZeroLevelConstructor::DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) {
    if (!proto.HasZeroLevel()) {
        return true;
    }
    auto& pLevel = proto.GetZeroLevel();
    if (pLevel.HasPortionsLiveDurationSeconds()) {
        PortionsLiveDuration = TDuration::Seconds(pLevel.GetPortionsLiveDurationSeconds());
    }
    if (pLevel.HasExpectedBlobsSize()) {
        ExpectedBlobsSize = pLevel.GetExpectedBlobsSize();
    }
    if (pLevel.HasPortionsCountAvailable()) {
        PortionsCountAvailable = pLevel.GetPortionsCountAvailable();
    }
    if (pLevel.HasPortionsCountLimit()) {
        PortionsCountLimit = pLevel.GetPortionsCountLimit();
    }
    if (pLevel.HasPortionsSizeLimit()) {
        PortionsSizeLimit = pLevel.GetPortionsSizeLimit();
    }
    if (pLevel.HasDataSnapshotInterval()) {
        auto conclusion = DataSnapshotInterval.DeserializeFromProto(pLevel.GetDataSnapshotInterval());
        if (conclusion.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot parse compaction level")("error", conclusion.GetErrorMessage());
            return false;
        }
    }
    return true;
}

void TZeroLevelConstructor::DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const {
    auto& mLevel = *proto.MutableZeroLevel();
    if (PortionsLiveDuration) {
        mLevel.SetPortionsLiveDurationSeconds(PortionsLiveDuration->Seconds());
    }
    if (ExpectedBlobsSize) {
        mLevel.SetExpectedBlobsSize(*ExpectedBlobsSize);
    }
    if (PortionsCountAvailable) {
        mLevel.SetPortionsCountAvailable(*PortionsCountAvailable);
    }
    if (PortionsCountLimit) {
        mLevel.SetPortionsCountLimit(*PortionsCountLimit);
    }
    if (PortionsSizeLimit) {
        mLevel.SetPortionsSizeLimit(*PortionsSizeLimit);
    }
    if (!DataSnapshotInterval.IsEmpty()) {
        *mLevel.MutableDataSnapshotInterval() = DataSnapshotInterval.SerializeToProto();
    }
}

std::shared_ptr<NKikimr::NOlap::NStorageOptimizer::NLCBuckets::IPortionsLevel> TZeroLevelConstructor::DoBuildLevel(
    const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const std::shared_ptr<TSimplePortionsGroupInfo>& /*portionsInfo*/,
    const TLevelCounters& counters) const {
    return std::make_shared<TZeroLevelPortions>(indexLevel, nextLevel, counters, PortionsLiveDuration.value_or(TDuration::Max()),
        ExpectedBlobsSize.value_or((ui64)1 << 20), PortionsCountAvailable.value_or(10), PortionsCountLimit, PortionsSizeLimit,
        DataSnapshotInterval);
}

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
