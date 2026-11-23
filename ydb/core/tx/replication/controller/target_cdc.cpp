#include "target_cdc.h"
#include "dst_creator.h"
#include "schema.h"

#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

using TCdc = NKikimrReplicationController::TCdcTarget;

TTargetCdc::TTargetCdc(TReplication* replication, ui64 id, const IConfig::TPtr& config)
    : TTargetTopic(replication, id, config)
{}

TCdc& TTargetCdc::GetCdc() { return Cdc; }
const TCdc& TTargetCdc::GetCdc() const { return Cdc; }

bool TTargetCdc::ActivateOnDetach() const {
    return Cdc.GetActivateOnDetach();
}

bool TTargetCdc::OnDstAltered(NIceDb::TNiceDb& db, const TActorContext& ctx) {
    switch (Cdc.GetPhase()) {
    case TCdc::STOPPING:
        Cdc.SetPhase(TCdc::TRIMMING);
        break;
    case TCdc::STOPPING_PRESERVE:
        Cdc.SetPhase(TCdc::PREPARED);
        break;
    case TCdc::TRIMMING:
        if (GetReplication()->GetState() == TReplication::EState::Removing) {
            Cdc.SetPhase(TCdc::FINISHED);
            SetDstState(EDstState::Done);
        } else {
            Cdc.SetPhase(TCdc::PREPARED);
        }
        break;
    default:
        return false;
    }

    Shutdown(ctx);
    db.Table<TControllerSchema::Targets>().Key(GetReplication()->GetId(), GetId()).Update(
        NIceDb::TUpdate<TControllerSchema::Targets::Cdc>(Cdc.SerializeAsString()));
    return true;
}

bool TTargetCdc::IsExpectedDstCreator(const TActorId& actor) const {
    return (HandoffActor && HandoffActor == actor) || TTargetTopic::IsExpectedDstCreator(actor);
}

void TTargetCdc::Progress(const TActorContext& ctx) {
    auto* replication = GetReplication();
    if (GetDstState() == EDstState::Creating && Cdc.HasInitialCreateTxId()) {
        if (!HandoffActor) {
            HandoffActor = ctx.Register(CreateDstCreator(replication, GetId(), ctx));
        }
        return;
    }
    if (GetDstState() == EDstState::Creating && GetStreamState() == EStreamState::Ready) {
        const auto* table = replication->FindTarget(Cdc.GetParentTargetId());
        if (!table || !table->GetDstPathId()
            || (table->GetDstState() != EDstState::Ready && table->GetDstState() != EDstState::Paused
                && table->GetDstState() != EDstState::PreparingCdc))
        {
            return;
        }
    }

    if (GetDstState() == EDstState::Alter) {
        switch (Cdc.GetPhase()) {
        case TCdc::RUNNING:
            if (replication->GetDesiredState() == TReplication::EState::Done) {
                return; // No CDC handoff policy has been admitted.
            }
            break;
        case TCdc::PREPARED:
        case TCdc::FINISHED:
            return;
        default:
            break;
        }
    }
    TTargetTopic::Progress(ctx);
}

void TTargetCdc::Shutdown(const TActorContext& ctx) {
    Reset(HandoffActor);
    TTargetTopic::Shutdown(ctx);
}

TTargetCdc* GetCdcTarget(TReplication::ITarget* target) {
    return target && target->GetKind() == TReplication::ETargetKind::CdcTopic
        ? static_cast<TTargetCdc*>(target) : nullptr;
}

const TTargetCdc* GetCdcTarget(const TReplication::ITarget* target) {
    return target && target->GetKind() == TReplication::ETargetKind::CdcTopic
        ? static_cast<const TTargetCdc*>(target) : nullptr;
}

TVector<TTargetCdc*> GetCdcChildren(const TReplication& replication, ui64 parentId) {
    TVector<TTargetCdc*> result;
    for (auto* target : replication.GetTargets()) {
        if (auto* cdc = GetCdcTarget(target); cdc && cdc->GetCdc().GetParentTargetId() == parentId) {
            result.push_back(cdc);
        }
    }
    return result;
}

void FinishCdcChildren(const TReplication& replication, ui64 parentId, NIceDb::TNiceDb& db) {
    for (auto* child : GetCdcChildren(replication, parentId)) {
        if (child->GetCdc().GetPhase() == TCdc::FINISHED) {
            continue;
        }
        Y_ABORT_UNLESS(child->GetCdc().GetPhase() == TCdc::PREPARED);
        child->GetCdc().SetPhase(TCdc::FINISHED);
        child->SetDstState(TReplication::EDstState::Done);
        db.Table<TControllerSchema::Targets>().Key(replication.GetId(), child->GetId()).Update(
            NIceDb::TUpdate<TControllerSchema::Targets::DstState>(child->GetDstState()),
            NIceDb::TUpdate<TControllerSchema::Targets::Cdc>(child->GetCdc().SerializeAsString()));
    }
}

}
