#include "abstract.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

    void TNormalizationController::RegisterNormalizer(INormalizerComponent::TPtr normalizer) {
        AFL_VERIFY(normalizer);
        Counters.emplace_back(normalizer->GetName());
        Normalizers.push_back(normalizer);
    }

    const TNormalizationController::INormalizerComponent::TPtr& TNormalizationController::GetNormalizer() const {
        Y_ABORT_UNLESS(CurrentNormalizerIndex < Normalizers.size());
        return Normalizers[CurrentNormalizerIndex];
    }

    const TNormalizerCounters& TNormalizationController::GetCounters() const {
        Y_ABORT_UNLESS(CurrentNormalizerIndex < Normalizers.size());
        return Counters[CurrentNormalizerIndex];
    }

    bool TNormalizationController::TNormalizationController::IsNormalizationFinished() const {
        return CurrentNormalizerIndex >= Normalizers.size();
    }

    bool TNormalizationController::SwitchNormalizer() {
        if (IsNormalizationFinished()) {
            return false;
        }
        Y_ABORT_UNLESS(!GetNormalizer()->HasActiveTasks());
        GetCounters().OnNormalizerFinish();
        ++CurrentNormalizerIndex;
        return !IsNormalizationFinished();
    }

    void TTrivialNormalizerTask::Start(const TNormalizationController& /* controller */, const TNormalizationContext& nCtx) {
        TActorContext::AsActorContext().Send(nCtx.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(Changes));
    }

    void TNormalizationController::UpdateControllerState(NIceDb::TNiceDb& db) {
        NColumnShard::Schema::SaveSpecialValue(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, GetNormalizer()->GetSequentialId());
    }

    void TNormalizationController::InitNormalizers(const TInitContext& ctx) {
        auto normalizers = GetEnumAllValues<ENormalizerSequentialId>();
        auto lastRegisteredNormalizer = ENormalizerSequentialId::Granules;
        for (auto nType : normalizers) {
            RegisterNormalizer(std::shared_ptr<INormalizerComponent>(INormalizerComponent::TFactory::Construct(nType, ctx)));
            AFL_VERIFY(lastRegisteredNormalizer <= nType)("current", ToString(nType))("last", ToString(lastRegisteredNormalizer));
            lastRegisteredNormalizer = nType;
        }
    }

    void TNormalizationController::InitControllerState(NIceDb::TNiceDb& db) {
        ui64 lastNormalizerId;
        if (NColumnShard::Schema::GetSpecialValue(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, lastNormalizerId)) {
            // We want to rerun all normalizers in case of binary rollback
            if (lastNormalizerId <= GetLastNormalizerSequentialId()) {
                LastAppliedNormalizerId = lastNormalizerId;
            }
        }
    }

}
