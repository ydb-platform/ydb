#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TPtr ev) {
    const auto& r = ev->Get()->Record;
    if (const auto it = LocalVDisks.find(r.GetVSlotId()); it != LocalVDisks.end() && ev->Cookie == it->second.ScrubCookie) {
        TVDiskRecord& vdisk = it->second;
        switch (vdisk.ScrubState) {
            case TVDiskRecord::EScrubState::IDLE:
                vdisk.ScrubState = TVDiskRecord::EScrubState::QUERY_START_QUANTUM;
                break;

            case TVDiskRecord::EScrubState::QUANTUM_FINISHED:
                vdisk.ScrubState = TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE;
                break;

            case TVDiskRecord::EScrubState::QUERY_START_QUANTUM:
            case TVDiskRecord::EScrubState::IN_PROGRESS:
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE:
                Y_ABORT("inconsistent ScrubState# %" PRIu32, vdisk.ScrubState);
        }
        // issue request to BS_CONTROLLER
        const TVSlotId vslotId = it->first;
        vdisk.ScrubCookieForController = ++LastScrubCookie;
        SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQueryStartQuantum>(
            vslotId.NodeId, vslotId.PDiskId, vslotId.VDiskSlotId), vdisk.ScrubCookieForController);
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerScrubStartQuantum::TPtr ev) {
    const auto& r = ev->Get()->Record;
    if (const auto it = LocalVDisks.find(r.GetVSlotId()); it != LocalVDisks.end() && ev->Cookie == it->second.ScrubCookieForController) {
        const TVSlotId vslotId = it->first;
        TVDiskRecord& vdisk = it->second;
        switch (vdisk.ScrubState) {
            case TVDiskRecord::EScrubState::IDLE:
            case TVDiskRecord::EScrubState::IN_PROGRESS:
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED:
                Y_ABORT("inconsistent ScrubState# %" PRIu32, vdisk.ScrubState);

            case TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE:
                vdisk.QuantumFinished.Clear();
                [[fallthrough]];
            case TVDiskRecord::EScrubState::QUERY_START_QUANTUM:
                vdisk.ScrubState = TVDiskRecord::EScrubState::IN_PROGRESS;
                TActivationContext::Send(ev->Forward(MakeBlobStorageVDiskID(vslotId.NodeId, vslotId.PDiskId, vslotId.VDiskSlotId)));
                break;
        }
    }
}

void TNodeWarden::Handle(TEvBlobStorage::TEvControllerScrubQuantumFinished::TPtr ev) {
    const auto& r = ev->Get()->Record;
    if (const auto it = LocalVDisks.find(r.GetVSlotId()); it != LocalVDisks.end() && ev->Cookie == it->second.ScrubCookie) {
        TVDiskRecord& vdisk = it->second;
        switch (vdisk.ScrubState) {
            case TVDiskRecord::EScrubState::IN_PROGRESS:
                vdisk.ScrubState = TVDiskRecord::EScrubState::QUANTUM_FINISHED;
                vdisk.QuantumFinished.CopyFrom(r);
                break;

            case TVDiskRecord::EScrubState::IDLE:
            case TVDiskRecord::EScrubState::QUERY_START_QUANTUM:
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED:
            case TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE:
                Y_ABORT("inconsistent ScrubState# %" PRIu32, vdisk.ScrubState);
        }
        // forward request to BS_CONTROLLER
        SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQuantumFinished>(r));
    }
}

void TNodeWarden::SendScrubRequests() {
    for (auto& [key, vdisk] : LocalVDisks) {
        switch (vdisk.ScrubState) {
            case TVDiskRecord::EScrubState::IDLE:
                // VDisk didn't ask for any scrub activity yet, so just keep calm
                break;

            case TVDiskRecord::EScrubState::QUERY_START_QUANTUM:
                vdisk.ScrubCookieForController = ++LastScrubCookie;
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQueryStartQuantum>(
                    key.NodeId, key.PDiskId, key.VDiskSlotId), vdisk.ScrubCookieForController);
                break;

            case TVDiskRecord::EScrubState::IN_PROGRESS:
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubReportQuantumInProgress>(
                    key.NodeId, key.PDiskId, key.VDiskSlotId));
                break;

            case TVDiskRecord::EScrubState::QUANTUM_FINISHED:
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQuantumFinished>(
                    vdisk.QuantumFinished));
                break;

            case TVDiskRecord::EScrubState::QUANTUM_FINISHED_AND_WAITING_FOR_NEXT_ONE:
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQuantumFinished>(
                    vdisk.QuantumFinished));
                vdisk.ScrubCookieForController = ++LastScrubCookie;
                SendToController(std::make_unique<TEvBlobStorage::TEvControllerScrubQueryStartQuantum>(
                    key.NodeId, key.PDiskId, key.VDiskSlotId), vdisk.ScrubCookieForController);
                break;
        }
    }
}
