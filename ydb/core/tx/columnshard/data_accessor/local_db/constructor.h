#pragma once
#include <ydb/core/tx/columnshard/data_accessor/abstract/constructor.h>

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

class TManagerConstructor: public IManagerConstructor {
public:
    static TString GetClassNameStatic() {
        return "local_db";
    }

private:
    virtual std::shared_ptr<IMetadataMemoryManager> DoBuild(const TManagerConstructionContext& context) const override;
    static const inline TFactory::TRegistrator<TManagerConstructor> Registrator =
        TFactory::TRegistrator<TManagerConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
