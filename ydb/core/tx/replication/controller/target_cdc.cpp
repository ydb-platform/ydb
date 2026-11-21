#include "target_cdc.h"
#include "dst_cdc_creator.h"
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

bool TTargetCdc::OnMirrorStopped(NIceDb::TNiceDb& db, const TActorContext& ctx) {
    if (Cdc.GetPhase() != TCdc::STOPPING && Cdc.GetPhase() != TCdc::STOPPING_PRESERVE) {
        return false;
    }

    Shutdown(ctx);
    Cdc.SetPhase(Cdc.GetPhase() == TCdc::STOPPING_PRESERVE ? TCdc::PREPARED : TCdc::RESETTING);
    db.Table<TControllerSchema::Targets>().Key(GetReplication()->GetId(), GetId()).Update(
        NIceDb::TUpdate<TControllerSchema::Targets::Cdc>(Cdc.SerializeAsString()));
    return true;
}

bool TTargetCdc::IsExpectedDstCreator(const TActorId& actor) const {
    return (HandoffActor && HandoffActor == actor) || TTargetTopic::IsExpectedDstCreator(actor);
}

void TTargetCdc::Progress(const TActorContext& ctx) {
    auto* replication = GetReplication();
    if (GetDstState() == EDstState::Alter) {
        if (Cdc.GetPhase() == TCdc::RESETTING) {
            for (const auto* sibling : GetCdcChildren(*replication, Cdc.GetParentTargetId())) {
                if (sibling->GetId() < GetId()
                    && sibling->GetCdc().GetPhase() != TCdc::PREPARED
                    && sibling->GetCdc().GetPhase() != TCdc::FINISHED)
                {
                    return;
                }
            }
        }
        switch (Cdc.GetPhase()) {
        case TCdc::RESETTING:
            if (!HandoffActor) {
                HandoffActor = ctx.Register(CreateDstCdcResetter(ctx.SelfID, replication->GetSchemeShardId(),
                    replication->GetDatabase(), replication->GetPathId(), replication->GetId(), GetId(),
                    TDstCdcResetSettings{
                        .TablePathId = TPathId::FromProto(Cdc.GetTablePathId()),
                        .StreamPathId = TPathId::FromProto(Cdc.GetStreamPathId()),
                        .TopicPathId = GetDstPathId(),
                        .CreateTxId = Cdc.GetResetCreateTxId(),
                        .Definition = Cdc.GetDefinition(),
                        .ActivateOnDetach = ActivateOnDetach(),
                    }));
            }
            return;
        case TCdc::RUNNING:
            if (replication->GetDesiredState() == TReplication::EState::Done) {
                return; // No explicit reset policy has been admitted.
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
        Y_ABORT_UNLESS(child->GetCdc().GetPhase() == TCdc::PREPARED);
        child->GetCdc().SetPhase(TCdc::FINISHED);
        child->SetDstState(TReplication::EDstState::Done);
        db.Table<TControllerSchema::Targets>().Key(replication.GetId(), child->GetId()).Update(
            NIceDb::TUpdate<TControllerSchema::Targets::DstState>(child->GetDstState()),
            NIceDb::TUpdate<TControllerSchema::Targets::Cdc>(child->GetCdc().SerializeAsString()));
    }
}

}
