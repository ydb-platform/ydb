#pragma once

#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {
class TLeakedBlobsNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return "LeakedBlobsNormalizer";
    }

private:
    static inline TFactory::TRegistrator<TLeakedBlobsNormalizer> Registrator =
        TFactory::TRegistrator<TLeakedBlobsNormalizer>(GetClassNameStatic());

public:
    class TNormalizerResult;
    class TTask;

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info);

    TConclusionStatus LoadPortionBlobIds(const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashSet<TLogoBlobID>& result);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    TVector<TTabletChannelInfo> Channels;
    TActorId TRemoveLeakedBlobsActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    THashMap<ui64, std::unique_ptr<TPortionInfoConstructor>> Portions;
    THashMap<ui64, TColumnChunkLoadContextV2::TBuildInfo> Records;
    THashMap<ui64, std::vector<TIndexChunkLoadContext>> Indexes;
    THashSet<TUnifiedBlobId> BlobsToDelete;
};
}   // namespace NKikimr::NOlap
