#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/update.h>

namespace NKikimr::NSchemeShard::NOperations {

class TTieringRuleUpdate: public TMetadataUpdate {
protected:
    static TString GetStorageDirectory();
    static TConclusionStatus ApplySettings(const NKikimrSchemeOp::TTieringRuleDescription& settings, TTieringRuleInfo::TPtr object);
    static TConclusionStatus ValidateTieringRule(const TTieringRuleInfo::TPtr& object);
    static void PersistTieringRule(const TPathId& pathId, const TTieringRuleInfo::TPtr& tieringRule, const TUpdateStartContext& context);

public:
    TPathElement::EPathType GetObjectPathType() const override {
        return NKikimrSchemeOp::EPathTypeTieringRule;
    }

    std::shared_ptr<ISSEntity> MakeEntity(const TPathId& pathId) const override;

    TTieringRuleUpdate(const TString& objectPath)
        : TMetadataUpdate(objectPath) {
    }
};

class TCreateTieringRule: public TTieringRuleUpdate {
private:
    using TBase = TTieringRuleUpdate;
    TTieringRuleInfo::TPtr UpdatedTieringRule;
    bool Exists;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoExecute(const TUpdateStartContext& context) override;

public:
    using TTieringRuleUpdate::TTieringRuleUpdate;
};

class TAlterTieringRule: public TTieringRuleUpdate {
private:
    using TBase = TTieringRuleUpdate;
    TTieringRuleInfo::TPtr UpdatedTieringRule;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoExecute(const TUpdateStartContext& context) override;

public:
    using TTieringRuleUpdate::TTieringRuleUpdate;
};

class TDropTieringRule: public TTieringRuleUpdate, public IDropMetadataUpdate {
private:
    using TBase = TTieringRuleUpdate;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoExecute(const TUpdateStartContext& context) override;

    void RestoreDrop(const TRestoreContext& /*context*/) override {
    }
    TConclusionStatus FinishDrop(const TUpdateFinishContext& context) override;

public:
    using TTieringRuleUpdate::TTieringRuleUpdate;
};

}   // namespace NKikimr::NSchemeShard::NOperations
