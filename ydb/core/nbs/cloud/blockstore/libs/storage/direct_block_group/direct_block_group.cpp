#include "direct_block_group.h"

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    ui64 tabletId, ui32 generation, const TVector<TDDiskId>& ddisksIds,
    const TVector<TDDiskId>& persistentBufferDDiskIds)
{
    auto addDDiskConnections = [&](const TVector<TDDiskId>& ddisksIds,
                                   TVector<TDDiskConnection>& ddiskConnections)
    {
        for (const auto& ddiskId: ddisksIds) {
            ddiskConnections.emplace_back(
                ddiskId, TQueryCredentials(tabletId, generation));
        }
    };

    addDDiskConnections(ddisksIds, DDiskConnections);
    addDDiskConnections(persistentBufferDDiskIds, PersistentBufferConnections);
}

void TDirectBlockGroup::EstablishConnections(const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION, "EstablishConnections");

    for (size_t i = 0; i < DDiskConnections.size(); i++) {
        auto request =
            std::make_unique<TEvDDiskConnect>(DDiskConnections[i].Credentials);
        ctx.Send(
            DDiskConnections[i].GetServiceId(),
            request.release(),
            0,   // flags
            i);
    };
}

void TDirectBlockGroup::HandleDDiskConnectResult(
    const NDDisk::TEvDDiskConnectResult::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "HandleDDiskConnectResult: " << ev->ToString());

    const auto* msg = ev->Get();

    Y_ABORT_UNLESS(ev->Cookie < DDiskConnections.size());
    auto& ddiskConnection = DDiskConnections[ev->Cookie];

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
        ddiskConnection.Credentials.DDiskInstanceGuid =
            msg->Record.GetDDiskInstanceGuid();
    } else {
        LOG_ERROR(
            ctx, NKikimrServices::NBS_PARTITION,
            "HandleDDiskConnectResult finished with error: %s, reason: %s",
            msg->Record.GetStatus(), msg->Record.GetErrorReason().data());
    }
}

void TDirectBlockGroup::HandleWriteBlocksRequest(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "HandleWriteBlocksRequest: " << ev->ToString());

    const auto* msg = ev->Get();
    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();

    size_t totalSize = 0;
    for (const auto& buffer: blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    // Round up
    // Move to separate function
    totalSize = (totalSize + 4096 - 1) / 4096 * 4096;

    TString data = TString::Uninitialized(totalSize);
    char* ptr = data.Detach();
    for (const auto& buffer: blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data.end() - ptr);

    auto request = std::make_shared<TWriteRequest>(ev->Sender, startIndex,
                                                   std::move(data));
    for (size_t i = 0; i < 3; i++) {
        auto requestToDDisk = std::make_unique<TEvDDiskWrite>(
            DDiskConnections[i].Credentials,
            TBlockSelector(0, request->StartIndex, request->GetDataSize()),
            TWriteInstruction(0));

        requestToDDisk->AddPayload(TRope(request->GetData()));

        ctx.Send(DDiskConnections[i].GetServiceId(), requestToDDisk.release(),
                 0, RequestId);
        RequestById[RequestId] = request;
        request->OnDDiskWriteRequested(RequestId, i);
        RequestId++;
    }
}

void TDirectBlockGroup::HandleDDiskWriteResult(
    const NDDisk::TEvDDiskWriteResult::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleDDiskWriteResult");

    const auto* msg = ev->Get();

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
        auto requestId = ev->Cookie;
        auto& request = RequestById[requestId];
        if (request->IsCompleted(requestId)) {
            auto response =
                std::make_unique<TEvService::TEvWriteBlocksResponse>();
            response->Record.MutableError()->CopyFrom(MakeError(S_OK));
            ctx.Send(request->Sender, std::move(response));

            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandleDDiskWriteResult finished with error: %s, reason: %s",
                  msg->Record.GetStatus(), msg->Record.GetErrorReason().data());
    }
}

void TDirectBlockGroup::HandleReadBlocksRequest(
    const TEvService::TEvReadBlocksRequest::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleReadBlocksRequest: " << ev->ToString());

    const auto* msg = ev->Get();
    const ui64 startIndex = msg->Record.GetStartIndex();
    const ui64 blocksCount = msg->Record.GetBlocksCount();

    auto request =
        std::make_shared<TReadRequest>(ev->Sender, startIndex, blocksCount);
    auto& ddiskConnection = DDiskConnections[0];

    auto requestToDDisk = std::make_unique<TEvDDiskRead>(
        ddiskConnection.Credentials,
        TBlockSelector(0, request->StartIndex, request->GetDataSize()),
        TReadInstruction(1));

    ctx.Send(DDiskConnections[0].GetServiceId(), requestToDDisk.release(), 0,
             RequestId);
    RequestById[RequestId] = request;
    RequestId++;
}

void TDirectBlockGroup::HandleDDiskReadResult(
    const NDDisk::TEvDDiskReadResult::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleDDiskWriteResult");

    const auto* msg = ev->Get();

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
        auto requestId = ev->Cookie;
        auto& request = RequestById[requestId];
        if (request->IsCompleted(requestId)) {
            Y_ABORT_UNLESS(msg->Record.HasReadResult());
            const auto& result = msg->Record.GetReadResult();
            Y_ABORT_UNLESS(result.HasPayloadId());
            Y_ABORT_UNLESS(result.GetPayloadId() == 0);
            TRope rope = msg->GetPayload(0);
            LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
                     "HandleDDiskReadResult finished with success, data: %s",
                     rope.ConvertToString().data());

            auto response =
                std::make_unique<TEvService::TEvReadBlocksResponse>();
            response->Record.MutableError()->CopyFrom(MakeError(S_OK));
            auto& blocks = *response->Record.MutableBlocks();
            blocks.AddBuffers(rope.ConvertToString());

            ctx.Send(request->Sender, std::move(response));

            RequestById.erase(requestId);
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
                  "HandleDDiskWriteResult finished with error: %s, reason: %s",
                  msg->Record.GetStatus(), msg->Record.GetErrorReason().data());
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
