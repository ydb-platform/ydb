#include "subsystem.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

TMockPDiskSubsystem::TMockPDiskSubsystem(TPDiskMockStates* states, TCreateState createState,
        TActorCreated actorCreated)
    : States(*states)
    , CreateState(std::move(createState))
    , ActorCreated(std::move(actorCreated))
{}

void TMockPDiskSubsystem::Start(const NActors::TActorContext& ctx, ui32 pdiskId,
        const TIntrusivePtr<TPDiskConfig>& config, const NPDisk::TMainKey&,
        ui32 poolId, ui32 nodeId) {
    auto& state = States[{nodeId, pdiskId}];
    if (!state) {
        state = CreateState(nodeId, pdiskId, *config);
        Y_ABORT_UNLESS(state);
    }
    const auto actorId = ctx.Register(CreatePDiskMockActor(state), NActors::TMailboxType::HTSwap, poolId);
    ctx.ActorSystem()->RegisterLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId), actorId);
    if (ActorCreated) {
        ActorCreated(actorId);
    }
}

} // namespace NKikimr
