#pragma once
#include <ydb/core/tx/columnshard/export/session/storage/abstract/storage.h>

namespace NKikimr::NOlap::NExport {

class TTierStorageInitializer: public IStorageInitializer {
public:
    static TString GetClassNameStatic() {
        return "TIER";
    }
private:
    TString TierName;
    static inline const TFactory::TRegistrator<TTierStorageInitializer> Registrator = TFactory::TRegistrator<TTierStorageInitializer>(GetClassNameStatic());
protected:
    virtual TConclusion<std::shared_ptr<IBlobsStorageOperator>> DoInitializeOperator(const std::shared_ptr<IStoragesManager>& storages) const override;
public:
    TTierStorageInitializer() = default;
    TTierStorageInitializer(const TString& tierName)
        : TierName(tierName)
    {

    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) override {
        if (!proto.HasTier()) {
            return TConclusionStatus::Fail("has not tier configuration");
        }
        TierName = proto.GetTier().GetTierName();
        return TConclusionStatus::Success();
    }

    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) const override {
        proto.MutableTier()->SetTierName(TierName);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};
}