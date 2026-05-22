#include "blob_recovery_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TBlobRecoveryActor::Handle(TEvRecoverBlob::TPtr ev) {
        auto *msg = ev->Get();
        const ui64 requestId = NextRequestId++;
        YDB_LOG_DEBUG(VDISKP(LogPrefix, "received TEvRecoverBlob"),
            {"Marker", "VDS31"},
            {"SelfId", SelfId()},
            {"Msg", ev->Get()->ToString()},
            {"IsConnected", IsConnected},
            {"WakeupScheduled", WakeupScheduled},
            {"RequestId", requestId});

        // create in flight context for this request and place it into in flight map
        auto context = std::make_shared<TInFlightContext>(requestId, *ev);
        context->Iterator = InFlight.emplace(msg->Deadline, context);
        context->Result->Deadline = msg->Deadline; // store original deadline in response

        // add origin items to result set
        auto& rItems = context->Result->Items;
        for (auto& item : msg->Items) {
            rItems.emplace_back(std::move(item));
            AddBlobQuery(rItems.back().BlobId, rItems.back().Needed, context, &rItems.back());
            ++context->NumUnrespondedBlobs;
        }

        // send queries to VDisks if necessary
        if (IsConnected) {
            SendPendingQueries();
        }

        // create timer to process deadlines if not yet created
        if (!WakeupScheduled) {
            Schedule(msg->Deadline, new TEvents::TEvWakeup);
            WakeupScheduled = true;
        }
    }

    void TBlobRecoveryActor::HandleWakeup() {
        const TInstant now = TActivationContext::Now();

        // process the in flight request queue and obtain next deadline
        TInFlight::iterator it;
        for (it = InFlight.begin(); it != InFlight.end() && it->first <= now; ++it) {
            for (auto& item : it->second->Result->Items) {
                if (item.Status == NKikimrProto::UNKNOWN) {
                    item.Status = NKikimrProto::DEADLINE;
                }
            }
            it->second->SendResult(SelfId());
        }
        InFlight.erase(InFlight.begin(), it);

        // reschedule timer
        if (it != InFlight.end()) {
            Schedule(it->first, new TEvents::TEvWakeup);
        } else {
            WakeupScheduled = false;
        }
    }

} // NKikimr
