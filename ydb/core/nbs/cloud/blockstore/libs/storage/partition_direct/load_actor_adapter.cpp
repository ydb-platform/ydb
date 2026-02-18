#include "load_actor_adapter.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

#include <ydb/library/actors/core/actorsystem.h>

using namespace NThreading;

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLoadActorAdapter::TLoadActorAdapter(
    std::shared_ptr<TFastPathService> fastPathService)
    : FastPathService(std::move(fastPathService))
{}

void TLoadActorAdapter::Bootstrap(const NActors::TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

///////////////////////////////////////////////////////////////////////////////
// !! LOAD ACTOR SHOULD GUARANTEE THAT THERE WILL BE NO MORE THAN ONE WRITE
// REQUEST TO THE SAME BLOCK AT A TIME !!
///////////////////////////////////////////////////////////////////////////////

void TLoadActorAdapter::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();

    size_t totalSize = 0;
    for (const auto& buffer: blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    // Round up
    // Move to separate function
    totalSize = (totalSize + BlockSize - 1) / BlockSize * BlockSize;

    Y_ABORT_UNLESS(totalSize == 4096);

    auto data = std::make_shared<TString>(TString::Uninitialized(totalSize));
    char* ptr = data->Detach();
    for (const auto& buffer: blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data->end() - ptr);

    TSgList sglist = {TBlockDataRef(data->data(), data->size())};

    auto request = std::make_shared<TWriteBlocksLocalRequest>(
        TRequestHeaders{},
        TBlockRange64::WithLength(startIndex, totalSize / BlockSize));
    request->Sglist = TGuardedSgList(std::move(sglist));

    auto future = FastPathService->WriteBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));

    future.Subscribe(
        [actorSystem = NActors::TActivationContext::ActorSystem(),
         sender = ev->Sender,
         selfId = ctx.SelfID,
         cookie = ev->Cookie,
         data](const NThreading::TFuture<TWriteBlocksLocalResponse>& f)
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
        });
}

void TLoadActorAdapter::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(msg->Record.GetBlocksCount() == 1);

    auto data = std::make_shared<TString>(TString::Uninitialized(4096));
    TSgList sglist = {TBlockDataRef(data->data(), data->size())};

    auto request = std::make_shared<TReadBlocksLocalRequest>(
        TRequestHeaders{},
        TBlockRange64::WithLength(
            msg->Record.GetStartIndex(),
            msg->Record.GetBlocksCount()));
    request->Sglist = TGuardedSgList(std::move(sglist));

    auto future = FastPathService->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        request);

    future.Subscribe(
        [actorSystem = NActors::TActivationContext::ActorSystem(),
         sender = ev->Sender,
         selfId = ctx.SelfID,
         cookie = ev->Cookie,
         request,
         data](const NThreading::TFuture<TReadBlocksLocalResponse>& f)
        {
            auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
                f.GetValue().Error);

            if (auto guard = request->Sglist.Acquire()) {
                const auto& sglist = guard.Get();
                for (const auto& block: sglist) {
                    response->Record.MutableBlocks()->AddBuffers(
                        block.Data(),
                        block.Size());
                }
            } else {
                Y_ABORT_UNLESS(false);
            }

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
        NActors::TActivationContext::AsActorContext(),
        NKikimrServices::NBS_PARTITION,
        "Processing event: %s from sender: %lu",
        ev->GetTypeName().data(),
        ev->Sender.LocalId());

    switch (ev->GetTypeRewrite()) {
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvService::TEvWriteBlocksRequest, HandleWriteBlocksRequest);
        HFunc(TEvService::TEvReadBlocksRequest, HandleReadBlocksRequest);

        default:
            LOG_DEBUG_S(
                NActors::TActivationContext::AsActorContext(),
                NKikimrServices::NBS_PARTITION,
                "Unhandled event type: " << ev->GetTypeRewrite()
                                         << " event: " << ev->ToString());
            break;
    }
}

///////////////////////////////////////////////////////////////////////////////

TActorId CreateLoadActorAdapter(
    const NActors::TActorId& owner,
    std::shared_ptr<TFastPathService> fastPathService)
{
    auto actor =
        std::make_unique<TLoadActorAdapter>(std::move(fastPathService));

    return NActors::TActivationContext::Register(
        actor.release(),
        owner,
        NActors::TMailboxType::ReadAsFilled,
        NKikimr::AppData()->SystemPoolId);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
