#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::OnYellowChannelsChanged() {
    if (!IsAnyChannelYellowStop()) {
        OverloadSubscribers.NotifyOverloadSubscribers(NOverload::ERejectReason::YellowChannels, SelfId(), TabletID());
    }
}

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    OverloadSubscribers.RemoveOverloadSubscriber(ev->Get()->Record.GetSeqNo(), ev->Recipient, ev->Sender);
}

} // namespace NKikimr::NColumnShard
