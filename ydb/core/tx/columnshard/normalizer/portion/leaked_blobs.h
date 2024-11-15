#pragma once

#include "normalizer.h"

#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {
class TLeakedBlobsNormalizer: public TNormalizationController::INormalizerComponent {
public:
    static TString GetClassNameStatic() {
        return "LeakedBlobsNormalizer";
    }

private:
    static inline TFactory::TRegistrator<TLeakedBlobsNormalizer> Registrator = TFactory::TRegistrator<TLeakedBlobsNormalizer>(
        GetClassNameStatic());

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

    TConclusionStatus LoadPortionBlobIds(
        const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db, THashSet<TLogoBlobID>& result);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    TVector<TTabletChannelInfo> Channels;
    ui64 TabletId;
    TActorId ActorId;
    TActorId TRemoveLeakedBlobsActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
};
} // namespace NKikimr::NOlap
