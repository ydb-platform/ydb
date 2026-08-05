#include "fast_path_service.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/sglist.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!FastPathService) {
        auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>(
            MakeError(E_REJECTED, "partition not ready"));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();

    ui32 totalSize = 0;
    for (const auto& buffer: blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    totalSize = AlignUp(totalSize, DefaultBlockSize);

    Y_ABORT_UNLESS(totalSize > 0);
    Y_ABORT_UNLESS(totalSize % DefaultBlockSize == 0);

    auto data = std::make_shared<TString>(TString::Uninitialized(totalSize));
    char* ptr = data->Detach();
    for (const auto& buffer: blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data->end() - ptr);

    TSgList sglist = {TBlockDataRef(data->data(), data->size())};

    auto request = std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = FastPathService->GetVolumeConfig(),
        .Range = TBlockRange64::WithLength(
            startIndex,
            totalSize / DefaultBlockSize)});
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

void TPartitionActor::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!FastPathService) {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
            MakeError(E_REJECTED, "partition not ready"));
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
        return;
    }

    const ui32 blocksCount = msg->Record.GetBlocksCount();
    Y_ABORT_UNLESS(blocksCount > 0);

    auto buffer = std::make_shared<TString>(
        TString::Uninitialized(blocksCount * DefaultBlockSize));
    TSgList sglist = {TBlockDataRef(buffer->data(), buffer->size())};

    auto request = std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = FastPathService->GetVolumeConfig(),
        .Range = TBlockRange64::WithLength(
            msg->Record.GetStartIndex(),
            blocksCount)});
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
