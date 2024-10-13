#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/abstract/update.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard::NOperations {

class TTieringRuleUpdateBase {
protected:
    static TString DoGetStorageDirectory();
    static TConclusionStatus ApplySettings(const NKikimrSchemeOp::TTieringRuleProperties& settings, TTieringRuleInfo::TPtr object);
    static TConclusionStatus ValidateTieringRule(const TTieringRuleInfo::TPtr& object);
    static void PersistTieringRule(const TPathId& pathId, const TTieringRuleInfo::TPtr& tieringRule, const TUpdateStartContext& context);
};

class TCreateTieringRule: public TMetadataUpdateCreate, private TTieringRuleUpdateBase {
private:
    static TFactory::TRegistrator<TCreateTieringRule> Registrator;

    TTieringRuleInfo::TPtr UpdatedTieringRule;
    bool Exists;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus Execute(const TUpdateStartContext& context) override;

public:
    TPathElement::EPathType GetObjectPathType() const override {
        return NKikimrSchemeOp::EPathTypeTieringRule;
    }

    std::shared_ptr<TMetadataEntity> MakeEntity(const TPathId& pathId) const override;
};

class TAlterTieringRule: public TMetadataUpdateAlter, private TTieringRuleUpdateBase  {
private:
    static TFactory::TRegistrator<TAlterTieringRule> Registrator;

    TTieringRuleInfo::TPtr UpdatedTieringRule;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus Execute(const TUpdateStartContext& context) override;

public:
};

class TDropTieringRule: public TMetadataUpdateDrop, private TTieringRuleUpdateBase  {
private:
    static TFactory::TRegistrator<TDropTieringRule> Registrator;

protected:
    TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus DoStart(const TUpdateStartContext& context) override;
    TConclusionStatus DoFinish(const TUpdateFinishContext& context) override;

public:
};

}   // namespace NKikimr::NSchemeShard::NOperations
