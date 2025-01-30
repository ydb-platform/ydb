#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

class TManagerConstructor: public IManagerConstructor {
public:
    static TString GetClassNameStatic() {
        return "in_mem";
    }

private:
    virtual bool IsEqualToWithSameClassName(const IManagerConstructor& /*item*/) const override {
        return true;
    }
    virtual TConclusion<std::shared_ptr<IMetadataMemoryManager>> DoBuild(const TManagerConstructionContext& context) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& /*jsonValue*/) override {
        return TConclusionStatus::Success();
    }
    virtual bool DoDeserializeFromProto(const TProto& /*proto*/) override {
        return true;
    }
    virtual void DoSerializeToProto(TProto& /*proto*/) const override {
        return;
    }

    static const inline TFactory::TRegistrator<TManagerConstructor> Registrator =
        TFactory::TRegistrator<TManagerConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
