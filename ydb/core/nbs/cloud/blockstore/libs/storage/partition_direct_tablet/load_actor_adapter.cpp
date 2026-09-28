#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

#include <ydb/core/base/appdata_fwd.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

using namespace NThreading;
using namespace NActors;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

// Matches NeedToSplitRequest in the vhost split wrapper: a range whose
// inclusive ends sit in different stripes would be routed to one vchunk
// by GetVChunkIndex, which only looks at the start block.
bool CrossesStripe(const TVolumeConfig& volumeConfig, TBlockRange64 range)
{
    const ui64 blocksPerStripe = volumeConfig.BlocksPerStripe;
    if (!blocksPerStripe) {
        return false;
    }
    return range.Start / blocksPerStripe != range.End / blocksPerStripe;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLoadActorAdapter::TLoadActorAdapter(
    std::shared_ptr<TFastPathService> fastPathService)
    : FastPathService(std::move(fastPathService))
{}

void TLoadActorAdapter::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

///////////////////////////////////////////////////////////////////////////////
// !! LOAD ACTOR SHOULD GUARANTEE THAT THERE WILL BE NO MORE THAN ONE WRITE
// REQUEST TO THE SAME BLOCK AT A TIME !!
// !! AND THAT A REQUEST NEVER CROSSES A STRIPE BOUNDARY !!
///////////////////////////////////////////////////////////////////////////////

void TLoadActorAdapter::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();
    const auto volumeConfig = FastPathService->GetVolumeConfig();
    const ui32 blockSize = volumeConfig->BlockSize;

    ui64 totalSize = 0;
    for (const auto& buffer: blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    totalSize = AlignUp(totalSize, static_cast<ui64>(blockSize));

    Y_ABORT_UNLESS(totalSize > 0);
    Y_ABORT_UNLESS(totalSize % blockSize == 0);

    const TBlockRange64 range =
        TBlockRange64::WithLength(startIndex, totalSize / blockSize);
    if (CrossesStripe(*volumeConfig, range)) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(
            MakeError(E_ARGUMENT, "range crosses a stripe boundary"));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    auto data = std::make_shared<TString>(TString::Uninitialized(totalSize));
    char* ptr = data->Detach();
    for (const auto& buffer: blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data->end() - ptr);

    TSgList sglist = {TBlockDataRef(data->data(), data->size())};

    auto request = std::make_shared<TWriteBlocksLocalRequest>(
        TRequestHeaders{.VolumeConfig = volumeConfig, .Range = range});
    request->Sglist = TGuardedSgList(std::move(sglist));

    auto future = FastPathService->WriteBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         sender = ev->Sender,
         selfId = ctx.SelfID,
         cookie = ev->Cookie,
         data = std::move(data)]   //
        (const NThreading::TFuture<TWriteBlocksLocalResponse>& f) mutable
        {
            auto response =
                std::make_unique<TEvService::TEvWriteBlocksResponse>(
                    f.GetValue().Error);

            actorSystem->Send(new IEventHandle(
                sender,
                selfId,
                response.release(),
                0,
                cookie));

            data.reset();
        });
}

void TLoadActorAdapter::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const ui32 blocksCount = msg->Record.GetBlocksCount();
    Y_ABORT_UNLESS(blocksCount > 0);

    const auto volumeConfig = FastPathService->GetVolumeConfig();
    const ui32 blockSize = volumeConfig->BlockSize;
    const TBlockRange64 range =
        TBlockRange64::WithLength(msg->Record.GetStartIndex(), blocksCount);
    if (CrossesStripe(*volumeConfig, range)) {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
            MakeError(E_ARGUMENT, "range crosses a stripe boundary"));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    auto buffer = std::make_shared<TString>(
        TString::Uninitialized(static_cast<size_t>(blocksCount) * blockSize));
    TSgList sglist = {TBlockDataRef(buffer->data(), buffer->size())};

    auto request = std::make_shared<TReadBlocksLocalRequest>(
        TRequestHeaders{.VolumeConfig = volumeConfig, .Range = range});
    request->Sglist = TGuardedSgList(std::move(sglist));

    auto future = FastPathService->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        request);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         sender = ev->Sender,
         selfId = ctx.SelfID,
         cookie = ev->Cookie,
         request,
         buffer = std::move(buffer)](
            const NThreading::TFuture<TReadBlocksLocalResponse>& f)
        {
            auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
                f.GetValue().Error);
            response->Record.MutableBlocks()->AddBuffers(std::move(*buffer));

            actorSystem->Send(new IEventHandle(
                sender,
                selfId,
                response.release(),
                0,
                cookie));
        });
}

///////////////////////////////////////////////////////////////////////////////

STFUNC(TLoadActorAdapter::StateWork)
{
    LOG_DEBUG(
        TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocksRequest);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksRequest);

        default:
            LOG_DEBUG_S(
                TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                                         << " event: " << ev->ToString());
            break;
    }
}

///////////////////////////////////////////////////////////////////////////////

TActorId CreateLoadActorAdapter(
    const TActorId& owner,
    std::shared_ptr<TFastPathService> fastPathService)
{
    auto actor =
        std::make_unique<TLoadActorAdapter>(std::move(fastPathService));

    return TActivationContext::Register(
        actor.release(),
        owner,
        TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
