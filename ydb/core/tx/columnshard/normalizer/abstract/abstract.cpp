#include "abstract.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

TNormalizationController::INormalizerComponent::TPtr TNormalizationController::RegisterNormalizer(INormalizerComponent::TPtr normalizer) {
    AFL_VERIFY(normalizer);
    Counters.emplace_back(normalizer->GetClassName());
    Normalizers.emplace_back(normalizer);
    return normalizer;
}

const TNormalizationController::INormalizerComponent::TPtr& TNormalizationController::GetNormalizer() const {
    Y_ABORT_UNLESS(Normalizers.size());
    return Normalizers.front();
}

const TNormalizerCounters& TNormalizationController::GetCounters() const {
    Y_ABORT_UNLESS(Counters.size());
    return Counters.front();
}

bool TNormalizationController::TNormalizationController::IsNormalizationFinished() const {
    AFL_VERIFY(Counters.size() == Normalizers.size());
    return Normalizers.empty();
}

bool TNormalizationController::SwitchNormalizer() {
    if (IsNormalizationFinished()) {
        return false;
    }
    Y_ABORT_UNLESS(!GetNormalizer()->HasActiveTasks());
    GetCounters().OnNormalizerFinish();
    Normalizers.pop_front();
    Counters.pop_front();
    return !IsNormalizationFinished();
}

void TTrivialNormalizerTask::Start(const TNormalizationController& /* controller */, const TNormalizationContext& nCtx) {
    TActorContext::AsActorContext().Send(nCtx.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(Changes));
}

void TNormalizationController::AddRepairInfo(NIceDb::TNiceDb& db, const TString& info) const {
    NColumnShard::Schema::AddRepairEvent(db, GetNormalizer()->GetUniqueId(), TInstant::Now(), GetNormalizer()->GetUniqueDescription(), "INFO:" + info);
}

void TNormalizationController::UpdateControllerState(NIceDb::TNiceDb& db) const {
    if (auto seqId = GetNormalizer()->GetSequentialId()) {
        NColumnShard::Schema::SaveSpecialValue(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, *seqId);
    }
    if (GetNormalizer()->GetIsRepair()) {
        NColumnShard::Schema::AddRepairEvent(db, GetNormalizer()->GetUniqueId(), TInstant::Now(), GetNormalizer()->GetUniqueDescription(), "FINISHED");
    }
}

void TNormalizationController::InitNormalizers(const TInitContext& ctx) {
    if (HasAppData()) {
        for (auto&& i : AppDataVerified().ColumnShardConfig.GetRepairs()) {
            AFL_VERIFY(i.GetDescription())("error", "repair normalization have to has unique description");
            if (FinishedRepairs.contains(i.GetDescription())) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("warning", "repair already processed")("description", i.GetDescription());
            } else {
                auto normalizer = RegisterNormalizer(std::shared_ptr<INormalizerComponent>(INormalizerComponent::TFactory::Construct(i.GetClassName(), ctx)));
                normalizer->SetIsRepair(true).SetUniqueDescription(i.GetDescription());
            }
        }
    }
    auto normalizers = GetEnumAllValues<ENormalizerSequentialId>();
    auto lastRegisteredNormalizer = ENormalizerSequentialId::Granules;
    for (auto nType : normalizers) {
        auto normalizer = RegisterNormalizer(std::shared_ptr<INormalizerComponent>(INormalizerComponent::TFactory::Construct(::ToString(nType), ctx)));
        AFL_VERIFY(normalizer->GetEnumSequentialIdVerified() == nType);
        AFL_VERIFY(lastRegisteredNormalizer <= nType)("current", ToString(nType))("last", ToString(lastRegisteredNormalizer));
        normalizer->SetUniqueDescription(normalizer->GetClassName());
        lastRegisteredNormalizer = nType;
    }
}

bool TNormalizationController::InitControllerState(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<NColumnShard::Schema::Repairs>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        THashSet<TString> descriptions;
        while (!rowset.EndOfSet()) {
            if (rowset.GetValue<NColumnShard::Schema::Repairs::Event>() != "FINISHED") {
                continue;
            }
            descriptions.emplace(rowset.GetValue<NColumnShard::Schema::Repairs::UniqueDescription>());
            if (!rowset.Next()) {
                return false;
            }
        }
        FinishedRepairs = descriptions;
    }

    ui64 lastNormalizerId;
    if (NColumnShard::Schema::GetSpecialValue(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, lastNormalizerId)) {
        // We want to rerun all normalizers in case of binary rollback
        if (lastNormalizerId <= GetLastNormalizerSequentialId()) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("last_normalizer_id", lastNormalizerId)("event", "restored");
            LastAppliedNormalizerId = lastNormalizerId;
        } else {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("last_normalizer_id", LastAppliedNormalizerId)("event", "not_restored");
        }
    } else {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("last_normalizer_id", LastAppliedNormalizerId)("event", "have not info");
    }
    return true;
}

}
