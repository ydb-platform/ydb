#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NColumnShard {
class TTablesManager;
}

namespace NKikimr::NOlap::NCopyBlobIdsToV2 {

class TNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::CopyBlobIdsToV2);
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::CopyBlobIdsToV2;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    class TNormalizerResult;

    static inline INormalizerComponent::TFactory::TRegistrator<TNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TNormalizer>(GetClassNameStatic());

public:
    TNormalizer(const TNormalizationController::TInitContext& info)
        : TBase(info)
        , DsGroupSelector(info.GetStorageInfo()) {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    NColumnShard::TBlobGroupSelector DsGroupSelector;
};
}   // namespace NKikimr::NOlap::NCopyBlobIdsToV2
