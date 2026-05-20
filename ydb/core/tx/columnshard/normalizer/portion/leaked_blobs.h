#pragma once

#include "normalizer.h"

#include <contrib/ydb/core/tx/columnshard/columnshard_schema.h>
#include <contrib/ydb/core/tx/columnshard/defs.h>
#include <contrib/ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

#include <util/datetime/base.h>

namespace NKikimr::NOlap {

class TLeakedBlobsNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    class TNormalizerResult;
    class TTask;
    static TString GetClassNameStatic() {
        return "LeakedBlobsNormalizer";
    }

private:
    static inline TFactory::TRegistrator<TLeakedBlobsNormalizer> Registrator =
        TFactory::TRegistrator<TLeakedBlobsNormalizer>(GetClassNameStatic());

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    struct TExperimentMetrics {
        TVector<ui64> LatencyMicros;
        TVector<ui32> Waits;
    };

    struct TPendingExperimentOperation {
        bool Active = false;
        bool FreshRange = false;
        bool UseIndexColumnsV2 = false;
        TInternalPathId PathId = TInternalPathId::FromRawValue(0);
        ui64 PortionId = 0;
        ui32 Waits = 0;
        TMonotonic StartedAt = TMonotonic::Zero();
    };

    TVector<TTabletChannelInfo> Channels;
    NColumnShard::TBlobGroupSelector DsGroupSelector;

    bool ParamsInitialized = false;
    size_t FreshPortionsWindow = 200000;
    size_t RequestsPerTablePerStage = 100;
    NActors::NLog::EPriority LogLevel = NActors::NLog::PRI_WARN;

    bool ExperimentInitialized = false;
    bool ExperimentFinished = false;
    ui64 NumberOfRestarts = 0;
    TVector<std::pair<TInternalPathId, ui64>> ExperimentPortionIds;
    size_t ExperimentFreshStartIndex = 0;
    size_t ExperimentStage = 0; // 0 = random over all/interleaved, 1 = random over fresh/interleaved, 2 = random over all/columns-then-portions, 3 = random over fresh/columns-then-portions, 4 = done
    size_t ExperimentIteration = 0;
    ui64 ExperimentRandomState = 1;
    TPendingExperimentOperation PendingExperimentOperation;
    TExperimentMetrics AllRowsColumnsMetrics;
    TExperimentMetrics AllRowsPortionsMetrics;
    TExperimentMetrics FreshRowsColumnsMetrics;
    TExperimentMetrics FreshRowsPortionsMetrics;
    TExperimentMetrics AllRowsColumnsSequentialMetrics;
    TExperimentMetrics AllRowsPortionsSequentialMetrics;
    TExperimentMetrics FreshRowsColumnsSequentialMetrics;
    TExperimentMetrics FreshRowsPortionsSequentialMetrics;

    void ReadParamsFromDescription();
    TConclusionStatus DoExperiment(NIceDb::TNiceDb& db);
    void PrintExperimentResult() const;
};
}   // namespace NKikimr::NOlap