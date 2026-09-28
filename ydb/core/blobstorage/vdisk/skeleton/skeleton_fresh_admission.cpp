#include "skeleton_fresh_admission.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>
#include <library/cpp/monlib/service/pages/templates.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_SKELETON

namespace NKikimr {

    TFreshAdmissionGate::TFreshAdmissionGate(TIntrusivePtr<TVDiskContext> vctx, TPDiskCtxPtr pdiskCtx,
            std::shared_ptr<THull> hull, TRedispatch redispatch)
        : VCtx(std::move(vctx))
        , PDiskCtx(std::move(pdiskCtx))
        , Hull(std::move(hull))
        , Redispatch(std::move(redispatch))
    {}

    TFreshAdmissionGate::EDecision TFreshAdmissionGate::Decide(const TFreshAdmission& admission,
            ESpaceColor refuseAtColor, bool housekeeping, const TActorContext& ctx) {
        if (admission.Empty()) {
            return EDecision::Admitted;
        }
        if (InFlight || Hull->IsFreshRotationPending(admission)) {
            return EDecision::Wait;
        }
        if (!Hull->PrepareFreshForAdmission(admission, ctx)) {
            return EDecision::Wait; // Cur is full and rotates out as soon as the records in flight land
        }
        const TFreshShortfall shortfall = Hull->GetFreshReservationShortfall(admission);
        if (!shortfall.Total()) {
            Hull->AdmitToFresh(admission);
            return EDecision::Admitted;
        }
        if (const auto& refused = RefusedAtColor[housekeeping]; refused && refuseAtColor <= *refused) {
            return EDecision::Refused;
        }
        if (shortfall.Total() > Max<ui32>()) {
            // More than one reservation can ask for, and more than any disk has to give.
            return EDecision::Refused;
        }

        InFlight = TReservation{shortfall, refuseAtColor, housekeeping};
        YDB_LOG_DEBUG_CTX(ctx, "Reserving chunks for Fresh",
            {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
            {"logoBlobs", shortfall.LogoBlobs},
            {"blocks", shortfall.Blocks},
            {"barriers", shortfall.Barriers},
            {"refuseAtColor", NKikimrBlobStorage::TPDiskSpaceColor::E_Name(refuseAtColor)},
            {"housekeeping", housekeeping},
            {"marker", "BSVSFA01"});
        ctx.Send(PDiskCtx->PDiskId, new NPDisk::TEvChunkReserve(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound,
            ui32(shortfall.Total()), housekeeping, refuseAtColor));
        return EDecision::Wait;
    }

    void TFreshAdmissionGate::Park(std::unique_ptr<IEventHandle> ev) {
        if (Draining) {
            // The event being handled again could not be decided: it stays at the head of the line.
            Parked.push_front(std::move(ev));
            StopDraining = true;
        } else {
            Parked.push_back(std::move(ev));
        }
    }

    void TFreshAdmissionGate::Land(const TFreshAdmission& admission, const TActorContext& ctx) {
        if (!admission.Empty()) {
            Hull->LandInFresh(admission, ctx);
        }
    }

    void TFreshAdmissionGate::Kick(const TActorContext& ctx) {
        if (!Draining) {
            Drain(ctx);
        }
    }

    void TFreshAdmissionGate::Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
        const auto *msg = ev->Get();
        Y_VERIFY_S(InFlight, VCtx->VDiskLogPrefix << "unexpected " << msg->ToString());
        const TReservation reservation = *std::exchange(InFlight, std::nullopt);

        if (msg->Status == NKikimrProto::OK) {
            Hull->AddFreshReservedChunks(reservation.Split, msg->ChunkIds);
            // A grant tells nothing of the bounds stricter than its own: a batch refused at its strictest bound
            // and admitted at the next is handled again without asking at the strictest one once more.
            for (auto& refused : RefusedAtColor) {
                if (refused && reservation.RefuseAtColor <= *refused) {
                    refused.reset();
                }
            }
        } else {
            Y_VERIFY_S(msg->Status == NKikimrProto::OUT_OF_SPACE, VCtx->VDiskLogPrefix << msg->ToString());
            YDB_LOG_NOTICE_CTX(ctx, "PDisk declined to reserve chunks for Fresh",
                {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                {"result", msg->ToString()},
                {"refuseAtColor", NKikimrBlobStorage::TPDiskSpaceColor::E_Name(reservation.RefuseAtColor)},
                {"housekeeping", reservation.Housekeeping},
                {"marker", "BSVSFA02"});
            auto remember = [&](std::optional<ESpaceColor>& refused) {
                refused = refused ? Max(*refused, reservation.RefuseAtColor) : reservation.RefuseAtColor;
            };
            // A housekeeping reservation is judged more leniently, so its refusal settles ordinary ones too.
            remember(RefusedAtColor[reservation.Housekeeping]);
            if (reservation.Housekeeping) {
                remember(RefusedAtColor[0]);
            }
        }

        Drain(ctx);
    }

    void TFreshAdmissionGate::Drain(const TActorContext& ctx) {
        while (!Parked.empty() && !InFlight) {
            std::unique_ptr<IEventHandle> ev = std::move(Parked.front());
            Parked.pop_front();
            Draining = true;
            StopDraining = false;
            Redispatch(std::move(ev), ctx);
            Draining = false;
            if (StopDraining) {
                break;
            }
        }
        if (Parked.empty()) {
            RefusedAtColor[0].reset();
            RefusedAtColor[1].reset();
        }
    }

    void TFreshAdmissionGate::RenderHtml(IOutputStream& str) const {
        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Fresh Admission";
                }
                DIV_CLASS("panel-body") {
                    str << "Waiting: " << Parked.size() << "<br>";
                    if (InFlight) {
                        str << "Reserving: " << InFlight->Split.Total() << " chunks, refused at "
                            << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(InFlight->RefuseAtColor) << "<br>";
                    }
                    for (bool housekeeping : {false, true}) {
                        if (const auto& refused = RefusedAtColor[housekeeping]) {
                            str << (housekeeping ? "Housekeeping" : "Ordinary") << " refused at "
                                << NKikimrBlobStorage::TPDiskSpaceColor::E_Name(*refused) << "<br>";
                        }
                    }
                }
            }
        }
    }

} // NKikimr
