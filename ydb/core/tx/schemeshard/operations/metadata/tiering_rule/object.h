#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/operations/metadata/abstract/object.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

namespace NKikimr::NSchemeShard::NOperations {

class TTieringRuleEntity: public TMetadataEntity {
private:
    static TFactory::TRegistrator<TTieringRuleEntity> Registrator;

private:
    using TBase = TMetadataEntity;
    YDB_READONLY_DEF(TTieringRuleInfo::TPtr, TieringRuleInfo);

    std::shared_ptr<TMetadataUpdateDrop> GetDropUpdate() const override;

protected:
    [[nodiscard]] TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;

    TTieringRuleEntity(const TPathId& pathId, const TTieringRuleInfo::TPtr& objectInfo)
        : TBase(pathId)
        , TieringRuleInfo(objectInfo) {
    }

public:
    TString GetClassName() const override {
        return "TIERING_RULE";
    }

public:
    TTieringRuleEntity(const TPathId& pathId)
        : TBase(pathId) {
    }
};
}