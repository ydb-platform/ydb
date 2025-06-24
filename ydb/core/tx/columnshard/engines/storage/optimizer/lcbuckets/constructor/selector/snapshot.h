#pragma once
#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets/planner/selector/snapshot.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TSnapshotSelectorConstructor: public ISelectorConstructor {
public:
    static TString GetClassNameStatic() {
        return "Snapshot";
    }

private:
    TDataSnapshotInterval Interval;

    virtual std::shared_ptr<IPortionsSelector> DoBuildSelector() const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const override;

    const static inline auto Registrator = TFactory::TRegistrator<TSnapshotSelectorConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
