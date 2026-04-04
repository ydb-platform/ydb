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

    TConclusionStatus LoadPortionBlobIds(const NColumnShard::TTablesManager& tablesManager, NIceDb::TNiceDb& db);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    TVector<TTabletChannelInfo> Channels;
    TActorId TRemoveLeakedBlobsActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;
    
    bool PortionsFinished = false;
    std::pair<TInternalPathId, ui64> PortionsNextKey = {TInternalPathId::FromRawValue(0), 0};
    THashSet<std::pair<TInternalPathId, ui64>> PortionIdsInPortions;
    ui64 TotalPortions = 0;
    
    ui64 StoppedOnPortion = 0;
    ui64 StoppedOnColumns = 0;
    ui64 StoppedOnIndexes = 0;
    ui64 StoppedOnBlobsToDelete = 0;

    bool DeleteBlobsFinished = false;

    THashSet<TLogoBlobID> Result;

    bool ComparePortionsAndColumns = false;
    bool PrechargeIndexColumnV2 = false;
    bool IndexColumnsV2CountFinished = false;
    ui64 IndexColumnsV2RowCount = 0;
    ui64 StoppedOnIndexColumnsV2 = 0;
    std::optional<std::pair<ui64, ui64>> IndexColumnsV2LastKey;
    THashSet<std::pair<TInternalPathId, ui64>> PortionIdsInIndexColumnsV2;

    bool ProcessPortion(
        const std::unique_ptr<TPortionInfoConstructor>& portion, 
        const NKikimrTxColumnShard::TIndexPortionMeta& metaProto,
        const NColumnShard::TTablesManager& tablesManager, 
        TDbWrapper& wrapper,
        NIceDb::TNiceDb& db 
    );

    bool FullScanIndexColumnsV2(NTabletFlatExecutor::TTransactionContext& txc);
};
}   // namespace NKikimr::NOlap
