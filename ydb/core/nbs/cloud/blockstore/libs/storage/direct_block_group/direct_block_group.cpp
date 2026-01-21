#include "direct_block_group.h"
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TDirectBlockGroup::TDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    const TVector<TDDiskId>& ddisksIds,
    const TVector<TDDiskId>& persistentBufferDDiskIds)
{
    auto addDDiskConnections = [&](const TVector<TDDiskId>& ddisksIds, TVector<std::unique_ptr<TDDiskConnection>>& ddiskConnections) {
        for (const auto& ddiskId : ddisksIds) {
            ddiskConnections.push_back(
                std::make_unique<TDDiskConnection>(
                    ddiskId,
                    TQueryCredentials(tabletId, generation)));
        }
    };

    addDDiskConnections(ddisksIds, DDiskConnections);
    addDDiskConnections(persistentBufferDDiskIds,   PersistentBufferConnections);
}

void TDirectBlockGroup::EstablishConnections(const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "EstablishConnections");

    for (size_t i = 0; i < DDiskConnections.size(); i++) {
        auto request = std::make_unique<TEvDDiskConnect>(DDiskConnections[i]->Credentials);
        ctx.Send(
            DDiskConnections[i]->GetServiceId(),
            request.release(),
            0, // flags
            i
        );
    };
}

void TDirectBlockGroup::HandleDDiskConnectResult(const NDDisk::TEvDDiskConnectResult::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleDDiskConnectResult: " << ev->ToString());

    const auto* msg = ev->Get();    

    Y_ABORT_UNLESS(ev->Cookie < DDiskConnections.size());
    auto& ddiskConnection = DDiskConnections[ev->Cookie];

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
        ddiskConnection->Credentials.DDiskInstanceGuid = msg->Record.GetDDiskInstanceGuid();
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "HandleDDiskConnectResult finished with error: %s, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }
}

void TDirectBlockGroup::HandleWriteBlocksRequest(const TEvService::TEvWriteBlocksRequest::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleWriteBlocksRequest: " << ev->ToString());

    const auto* msg = ev->Get();
    const ui64 startIndex = msg->Record.GetStartIndex();
    const auto& blocks = msg->Record.GetBlocks();

    size_t totalSize = 0;
    for (const auto& buffer : blocks.GetBuffers()) {
        totalSize += buffer.size();
    }

    // Round up
    totalSize = (totalSize + 4096 - 1) / 4096 * 4096;

    TString data = TString::Uninitialized(totalSize);
    char* ptr = data.Detach();
    for (const auto& buffer : blocks.GetBuffers()) {
        memcpy(ptr, buffer.data(), buffer.size());
        ptr += buffer.size();
    }
    memset(ptr, 0, data.end() - ptr);

    Requests.push_back(std::make_shared<TWriteRequest>(ev->Sender, startIndex, ERequestType::Write, std::move(data)));
    ProcessNextRequest(ctx);
}

void TDirectBlockGroup::ProcessNextRequest(const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "ProcessNextRequest");

    if (Requests.empty()) {
        return;
    }

    const auto& request = Requests.front();
    if (request->Type == ERequestType::Write) {
        SendWriteRequestsToDDisk(ctx, std::static_pointer_cast<TWriteRequest>(request));
    } else {
        // bad
        SendReadRequestsToDDisk(ctx, std::static_pointer_cast<TReadRequest>(request));
    }
}

void TDirectBlockGroup::SendWriteRequestsToDDisk(const TActorContext& ctx, const std::shared_ptr<TWriteRequest>& request)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "SendWriteRequestsToDDisk");

    for (size_t i = 0; i < 3; i++) {
        auto startIndexinBytes = request->StartIndex * 4096;

        auto requestToDDisk = std::make_unique<TEvDDiskWrite>(
            DDiskConnections[i]->Credentials,
            TBlockSelector(0, startIndexinBytes, static_cast<ui32>(request->Data.size())),
            TWriteInstruction(0)  
        );

        requestToDDisk->AddPayload(TRope(request->Data));

        // bad - new ptr to create
        RequestByStorageRequestId[StorageRequestId] = std::make_pair(request, StorageRequestId);
        ctx.Send(DDiskConnections[i]->GetServiceId(), requestToDDisk.release(), 0, StorageRequestId++);
    }
}

void TDirectBlockGroup::HandleDDiskWriteResult(const NDDisk::TEvDDiskWriteResult::TPtr& ev, const TActorContext& ctx)
{        
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleDDiskWriteResult");

    const auto* msg = ev->Get();

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
        auto& [request, ddiskIndex] = RequestByStorageRequestId[ev->Cookie];
        if (!(request->DDiskMask & (1 << ddiskIndex))) {
            request->DDiskMask |= (1 << ddiskIndex);
            request->AckCount++;
            if (request->AckCount == 3) {
                auto response = std::make_unique<TEvService::TEvWriteBlocksResponse>();
                response->Record.MutableError()->CopyFrom(MakeError(S_OK));
                ctx.Send(request->Sender, std::move(response));

                RequestByStorageRequestId.erase(ev->Cookie);
                Requests.pop_front();
            }
        }
    } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "HandleDDiskWriteResult finished with error: %s, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
    }
}

void TDirectBlockGroup::HandleReadBlocksRequest(const TEvService::TEvReadBlocksRequest::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleReadBlocksRequest: " << ev->ToString());

    const auto* msg = ev->Get();
    const ui64 startIndex = msg->Record.GetStartIndex();
    const ui64 blocksCount = msg->Record.GetBlocksCount();

    Requests.push_back(std::make_shared<TReadRequest>(ev->Sender, startIndex, ERequestType::Read, blocksCount));
    ProcessNextRequest(ctx);
}

void TDirectBlockGroup::SendReadRequestsToDDisk(const TActorContext& ctx, const std::shared_ptr<TReadRequest>& request)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "SendReadRequestsToDDisk");

    auto startIndexinBytes = request->StartIndex * 4096;

    auto& ddiskConnection = DDiskConnections[0];

    auto requestToDDisk = std::make_unique<TEvDDiskRead>(
        ddiskConnection->Credentials,
        TBlockSelector(0, startIndexinBytes, static_cast<ui32>(request->BlocksCount * 4096)),
        TReadInstruction(1)
    );

    RequestByStorageRequestId[StorageRequestId] = std::make_pair(request, StorageRequestId);
    ctx.Send(ddiskConnection->GetServiceId(), requestToDDisk.release(), 0, StorageRequestId++);
}

void TDirectBlockGroup::HandleDDiskReadResult(const NDDisk::TEvDDiskReadResult::TPtr& ev, const TActorContext& ctx)
{        
    LOG_DEBUG_S(ctx, NKikimrServices::NBS_PARTITION,
                "Handle HandleDDiskReadResult");

    const auto* msg = ev->Get();

    if (msg->Record.GetStatus() == NKikimrBlobStorage::TDDiskReplyStatus::OK) {
    // REFACTOR
        auto& [request, ddiskIndex] = RequestByStorageRequestId[ev->Cookie];
        if (!(request->DDiskMask & (1 << ddiskIndex))) {
            // REFACTOR
            request->DDiskMask |= (1 << ddiskIndex);
            request->AckCount++;
            // REFACTOR
            if (request->AckCount == 1) {
                Y_ABORT_UNLESS(msg->Record.HasReadResult());
                const auto& result = msg->Record.GetReadResult();
                Y_ABORT_UNLESS(result.HasPayloadId());
                Y_ABORT_UNLESS(result.GetPayloadId() == 0);
                TRope rope = msg->GetPayload(0);
                LOG_INFO(ctx, NKikimrServices::NBS_PARTITION,
                    "HandleDDiskReadResult finished with success, data: %s",
                    rope.ConvertToString().data());

                auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
                response->Record.MutableError()->CopyFrom(MakeError(S_OK));
                auto& blocks = *response->Record.MutableBlocks();
                // converting?? std::move??
                blocks.AddBuffers(rope.ConvertToString());

                ctx.Send(request->Sender, std::move(response));

                RequestByStorageRequestId.erase(ev->Cookie);
                Requests.pop_front();
            }
        } else {
        LOG_ERROR(ctx, NKikimrServices::NBS_PARTITION,
            "HandleDDiskWriteResult finished with error: %s, reason: %s",
            msg->Record.GetStatus(),
            msg->Record.GetErrorReason().data());
        }
    }
}

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
