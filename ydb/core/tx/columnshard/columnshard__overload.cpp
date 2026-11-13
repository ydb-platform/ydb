#include "columnshard_impl.h"

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvColumnShard::TEvOverloadUnsubscribe::TPtr& ev, const TActorContext&) {
    Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadUnsubscribe>(NOverload::TColumnShardInfo{ .ColumnShardId = SelfId(), .TabletId = TabletID() },
            NOverload::TOverloadSubscriberInfo{
                .PipeServerId = ev->Recipient, .OverloadSubscriberId = ev->Sender, .SeqNo = ev->Get()->Record.GetSeqNo() }));
}

}   // namespace NKikimr::NColumnShard
