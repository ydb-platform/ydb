#include "datashard_impl.h"

namespace NKikimr::NDataShard {

void TDataShard::Handle(TEvDataShard::TEvForceDataCleanup::TPtr& ev, const TActorContext& ctx) {
    // TODO: implement
    const auto& record = ev->Get()->Record;
    auto result = MakeHolder<TEvDataShard::TEvForceDataCleanupResult>(record.GetDataCleanupGeneration());
    ctx.Send(ev->Sender, std::move(result));
}

} // namespace NKikimr::NDataShard
