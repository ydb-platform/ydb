#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TDataSnapshotInterval {
private:
    YDB_READONLY_DEF(std::optional<TInstant>, StartInstant);
    YDB_READONLY_DEF(std::optional<TInstant>, FinishInstant);

public:
    bool IsEmpty() const {
        return !StartInstant && !FinishInstant;
    }

    bool operator==(const TDataSnapshotInterval& item) const = default;

    TDataSnapshotInterval() = default;
    TDataSnapshotInterval(const std::optional<TInstant>& start, const std::optional<TInstant>& finish)
        : StartInstant(start)
        , FinishInstant(finish) {
        AFL_VERIFY(!StartInstant || !FinishInstant || *StartInstant <= *FinishInstant);
    }

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonValue) {
        if (!jsonValue.IsMap()) {
            return TConclusionStatus::Fail("json have to be a map");
        }
        if (jsonValue.Has("start_seconds_utc")) {
            if (!jsonValue["start_seconds_utc"].IsUInteger()) {
                return TConclusionStatus::Fail("json start_seconds_utc value have to be unsigned int");
            }
            StartInstant = TInstant::Seconds(jsonValue["start_seconds_utc"].GetUInteger());
        }
        if (jsonValue.Has("finish_seconds_utc")) {
            if (!jsonValue["finish_seconds_utc"].IsUInteger()) {
                return TConclusionStatus::Fail("json finish_seconds_utc value have to be unsigned int");
            }
            FinishInstant = TInstant::Seconds(jsonValue["finish_seconds_utc"].GetUInteger());
        }
        return TConclusionStatus::Success();
    }

    template <class TProto>
    TConclusionStatus DeserializeFromProto(const TProto& proto) {
        if (proto.HasStartSecondsUTC()) {
            StartInstant = TInstant::Seconds(proto.GetStartSecondsUTC());
        }
        if (proto.HasFinishSecondsUTC()) {
            FinishInstant = TInstant::Seconds(proto.GetFinishSecondsUTC());
        }
        return TConclusionStatus::Success();
    }

    NKikimrSchemeOp::TCompactionSelectorConstructorContainer::TDataSnapshotInterval SerializeToProto() const {
        NKikimrSchemeOp::TCompactionSelectorConstructorContainer::TDataSnapshotInterval result;
        if (StartInstant) {
            result.SetStartSecondsUTC(StartInstant->Seconds());
        }
        if (FinishInstant) {
            result.SetFinishSecondsUTC(FinishInstant->Seconds());
        }
        return result;
    }

    bool CheckPortion(const TPortionInfo::TPtr& p) const {
        if (StartInstant && p->RecordSnapshotMax().GetPlanInstant() < *StartInstant) {
            return false;
        }
        if (FinishInstant && *FinishInstant <= p->RecordSnapshotMin().GetPlanInstant()) {
            return false;
        }
        return true;
    }
};

class TSnapshotPortionsSelector: public IPortionsSelector {
private:
    using TBase = IPortionsSelector;
    const TDataSnapshotInterval DataSnapshotInterval;

    virtual bool DoIsAppropriate(const TPortionInfo::TPtr& portionInfo) const override {
        return DataSnapshotInterval.CheckPortion(portionInfo);
    }

public:
    TSnapshotPortionsSelector(const TDataSnapshotInterval& dataSnapshotInterval, const TString& name)
        : TBase(name)
        , DataSnapshotInterval(dataSnapshotInterval) {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
