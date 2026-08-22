#include "ddisk_stub_actor.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

using namespace NActors;
using namespace NKikimr;

namespace {

using TReplyStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
using TPayloadKey = TDDiskStubState::TPayloadKey;
using EHeldKind = TDDiskStubState::EHeldKind;

[[nodiscard]] TRope ExtractPayload(const auto& event)
{
    if (event.GetPayloadCount() == 0) {
        return {};
    }
    return TRope(event.GetPayload(0));
}

[[nodiscard]] TPayloadKey MakeKey(
    const NKikimrBlobStorage::NDDisk::TBlockSelector& selector,
    ui64 lsn)
{
    return TPayloadKey{
        .VChunkIndex = selector.GetVChunkIndex(),
        .OffsetInBytes = selector.GetOffsetInBytes(),
        .Lsn = lsn};
}

[[nodiscard]] TVector<NKikimrBlobStorage::NDDisk::TDDiskId>
ExtractPersistentBufferIds(
    const NKikimrBlobStorage::NDDisk::TEvWritePersistentBuffers& record)
{
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> ids;
    ids.reserve(record.PersistentBufferIdsSize());
    for (const auto& id: record.GetPersistentBufferIds()) {
        ids.push_back(id);
    }
    return ids;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDDiskStubActor::TDDiskStubActor(TDDiskStubStatePtr state)
    : State(std::move(state))
{}

void TDDiskStubActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    Become(&TThis::StateWork);
}

void TDDiskStubActor::Hold(TDDiskStubState::THeldRequest held)
{
    auto guard = Guard(State->Lock);
    State->HeldRequests.push_back(std::move(held));
}

void TDDiskStubActor::StorePayload(const TPayloadKey& key, TRope payload)
{
    auto guard = Guard(State->Lock);
    State->Payloads[key] = std::move(payload);
}

TRope TDDiskStubActor::LoadPayload(const TPayloadKey& key) const
{
    auto guard = Guard(State->Lock);
    if (const auto* payload = State->Payloads.FindPtr(key)) {
        return *payload;
    }
    return {};
}

void TDDiskStubActor::ReplyWrite(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie)
{
    ctx.Send(sender, new NDDisk::TEvWriteResult(TReplyStatus::OK), 0, cookie);
}

void TDDiskStubActor::ReplyWritePBuffer(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie)
{
    ctx.Send(
        sender,
        new NDDisk::TEvWritePersistentBufferResult(TReplyStatus::OK),
        0,
        cookie);
}

void TDDiskStubActor::ReplyWritePBuffers(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie,
    const TVector<NKikimrBlobStorage::NDDisk::TDDiskId>& ids)
{
    bool split = false;
    {
        auto guard = Guard(State->Lock);
        split = State->SplitWriteToManyReplies && ids.size() > 1;
    }

    auto sendSlice = [&](size_t begin, size_t end)
    {
        auto response =
            std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
        for (size_t i = begin; i < end; ++i) {
            auto* res = response->Record.AddResult();
            *res->MutablePersistentBufferId() = ids[i];
            res->MutableResult()->SetStatus(TReplyStatus::OK);
        }
        ctx.Send(sender, response.release(), 0, cookie);
    };

    if (split) {
        const size_t mid = ids.size() / 2;
        sendSlice(0, mid);
        sendSlice(mid, ids.size());
    } else {
        sendSlice(0, ids.size());
    }
}

void TDDiskStubActor::ReplyRead(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie,
    const TPayloadKey& key,
    bool pbuffer)
{
    TRope data = LoadPayload(key);
    if (pbuffer) {
        ctx.Send(
            sender,
            new NDDisk::TEvReadPersistentBufferResult(
                TReplyStatus::OK,
                std::nullopt,
                key.VChunkIndex,
                key.OffsetInBytes,
                data.size(),
                std::move(data)),
            0,
            cookie);
    } else {
        ctx.Send(
            sender,
            new NDDisk::TEvReadResult(
                TReplyStatus::OK,
                std::nullopt,
                std::move(data)),
            0,
            cookie);
    }
}

void TDDiskStubActor::ReplyErase(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie)
{
    ctx.Send(
        sender,
        new NDDisk::TEvErasePersistentBufferResult(TReplyStatus::OK),
        0,
        cookie);
}

void TDDiskStubActor::ReplySync(
    const TActorContext& ctx,
    TActorId sender,
    ui64 cookie)
{
    ctx.Send(sender, new NDDisk::TEvSyncResult(TReplyStatus::OK), 0, cookie);
}

////////////////////////////////////////////////////////////////////////////////

void TDDiskStubActor::HandleConnect(
    const NDDisk::TEvConnect::TPtr& ev,
    const TActorContext& ctx)
{
    ui64 ddiskInstanceGuid = 0;
    {
        auto guard = Guard(State->Lock);
        State->ConnectCredentials.emplace_back(
            ev->Get()->Record.GetCredentials());
        if (State->PendingConnect) {
            return;
        }
        ddiskInstanceGuid = State->DDiskInstanceGuid;
    }

    auto response = std::make_unique<NDDisk::TEvConnectResult>(
        TReplyStatus::OK,
        std::nullopt,
        ddiskInstanceGuid);
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDDiskStubActor::HandleRead(
    const NDDisk::TEvRead::TPtr& ev,
    const TActorContext& ctx)
{
    const auto key = MakeKey(ev->Get()->Record.GetSelector(), /*lsn=*/0);
    {
        auto guard = Guard(State->Lock);
        if (State->PendingRead) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::Read,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Key = key,
            });
            return;
        }
    }
    ReplyRead(ctx, ev->Sender, ev->Cookie, key, /*pbuffer=*/false);
}

void TDDiskStubActor::HandleWrite(
    const NDDisk::TEvWrite::TPtr& ev,
    const TActorContext& ctx)
{
    const auto key = MakeKey(ev->Get()->Record.GetSelector(), /*lsn=*/0);
    auto payload = ExtractPayload(*ev->Get());
    {
        auto guard = Guard(State->Lock);
        if (State->PendingWrite) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::Write,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Payload = std::move(payload),
                .Key = key,
            });
            return;
        }
        State->Payloads[key] = std::move(payload);
    }
    ReplyWrite(ctx, ev->Sender, ev->Cookie);
}

void TDDiskStubActor::HandleWritePersistentBuffer(
    const NDDisk::TEvWritePersistentBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    const auto key = MakeKey(record.GetSelector(), record.GetLsn());
    auto payload = ExtractPayload(*ev->Get());
    {
        auto guard = Guard(State->Lock);
        if (State->PendingWritePBuffer) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::WritePBuffer,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Payload = std::move(payload),
                .Key = key,
            });
            return;
        }
        State->Payloads[key] = std::move(payload);
    }
    ReplyWritePBuffer(ctx, ev->Sender, ev->Cookie);
}

void TDDiskStubActor::HandleWritePersistentBuffers(
    const NDDisk::TEvWritePersistentBuffers::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    const auto key = MakeKey(record.GetSelector(), record.GetLsn());
    auto payload = ExtractPayload(*ev->Get());
    auto ids = ExtractPersistentBufferIds(record);
    {
        auto guard = Guard(State->Lock);
        if (State->PendingWritePBuffer) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::WritePBuffers,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Payload = std::move(payload),
                .Key = key,
                .PersistentBufferIds = ids,
            });
            return;
        }
        State->Payloads[key] = std::move(payload);
    }
    ReplyWritePBuffers(ctx, ev->Sender, ev->Cookie, ids);
}

void TDDiskStubActor::HandleReadPersistentBuffer(
    const NDDisk::TEvReadPersistentBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    const auto key = MakeKey(record.GetSelector(), record.GetLsn());
    {
        auto guard = Guard(State->Lock);
        if (State->PendingRead) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::ReadPBuffer,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
                .Key = key,
            });
            return;
        }
    }
    ReplyRead(ctx, ev->Sender, ev->Cookie, key, /*pbuffer=*/true);
}

void TDDiskStubActor::HandleBatchErasePersistentBuffer(
    const NDDisk::TEvBatchErasePersistentBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    {
        auto guard = Guard(State->Lock);
        if (State->PendingErase) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::Erase,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
            });
            return;
        }
    }
    ReplyErase(ctx, ev->Sender, ev->Cookie);
}

void TDDiskStubActor::HandleErasePersistentBuffer(
    const NDDisk::TEvErasePersistentBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    {
        auto guard = Guard(State->Lock);
        if (State->PendingErase) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::Erase,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
            });
            return;
        }
    }
    ReplyErase(ctx, ev->Sender, ev->Cookie);
}

void TDDiskStubActor::HandleSync(
    const NDDisk::TEvSync::TPtr& ev,
    const TActorContext& ctx)
{
    {
        auto guard = Guard(State->Lock);
        if (State->PendingSync) {
            State->HeldRequests.push_back(TDDiskStubState::THeldRequest{
                .Kind = EHeldKind::Sync,
                .Sender = ev->Sender,
                .Cookie = ev->Cookie,
            });
            return;
        }
    }
    ReplySync(ctx, ev->Sender, ev->Cookie);
}

void TDDiskStubActor::HandleListPersistentBuffer(
    const NDDisk::TEvListPersistentBuffer::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<NDDisk::TEvListPersistentBufferResult>(
        TReplyStatus::OK);
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

STFUNC(TDDiskStubActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NDDisk::TEvConnect, HandleConnect);
        HFunc(NDDisk::TEvRead, HandleRead);
        HFunc(NDDisk::TEvWrite, HandleWrite);
        HFunc(NDDisk::TEvWritePersistentBuffer, HandleWritePersistentBuffer);
        HFunc(NDDisk::TEvWritePersistentBuffers, HandleWritePersistentBuffers);
        HFunc(NDDisk::TEvReadPersistentBuffer, HandleReadPersistentBuffer);
        HFunc(
            NDDisk::TEvBatchErasePersistentBuffer,
            HandleBatchErasePersistentBuffer);
        HFunc(NDDisk::TEvErasePersistentBuffer, HandleErasePersistentBuffer);
        HFunc(NDDisk::TEvSync, HandleSync);
        HFunc(NDDisk::TEvListPersistentBuffer, HandleListPersistentBuffer);

        default:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ReleaseHeldWritePBuffersFirstHalf(
    TDDiskStubStatePtr state,
    TActorSystem* actorSystem)
{
    Y_ABORT_UNLESS(state);
    Y_ABORT_UNLESS(actorSystem);

    TDDiskStubState::THeldRequest request;
    bool found = false;
    {
        auto guard = Guard(state->Lock);
        for (auto& held: state->HeldRequests) {
            if (held.Kind == EHeldKind::WritePBuffers &&
                held.PersistentBufferIds.size() > 1)
            {
                request.Sender = held.Sender;
                request.Cookie = held.Cookie;
                request.Key = held.Key;
                request.Payload = held.Payload;
                const size_t mid = held.PersistentBufferIds.size() / 2;
                request.PersistentBufferIds.assign(
                    held.PersistentBufferIds.begin(),
                    held.PersistentBufferIds.begin() + mid);
                held.PersistentBufferIds.erase(
                    held.PersistentBufferIds.begin(),
                    held.PersistentBufferIds.begin() + mid);
                state->Payloads[request.Key] = request.Payload;
                found = true;
                break;
            }
        }
    }
    if (!found) {
        return;
    }

    auto response = std::make_unique<NDDisk::TEvWritePersistentBuffersResult>();
    for (const auto& id: request.PersistentBufferIds) {
        auto* res = response->Record.AddResult();
        *res->MutablePersistentBufferId() = id;
        res->MutableResult()->SetStatus(TReplyStatus::OK);
    }
    actorSystem->Send(new IEventHandle(
        request.Sender,
        TActorId(),
        response.release(),
        0,
        request.Cookie));
}

void ReleaseHeldRequests(
    TDDiskStubStatePtr state,
    TActorSystem* actorSystem,
    EHeldKind kind)
{
    Y_ABORT_UNLESS(state);
    Y_ABORT_UNLESS(actorSystem);

    TVector<TDDiskStubState::THeldRequest> held;
    {
        auto guard = Guard(state->Lock);
        for (auto it = state->HeldRequests.begin();
             it != state->HeldRequests.end();)
        {
            if (it->Kind == kind) {
                held.push_back(std::move(*it));
                it = state->HeldRequests.erase(it);
            } else {
                ++it;
            }
        }
        switch (kind) {
            case EHeldKind::Read:
            case EHeldKind::ReadPBuffer:
                state->PendingRead = false;
                break;
            case EHeldKind::Write:
                state->PendingWrite = false;
                break;
            case EHeldKind::WritePBuffer:
            case EHeldKind::WritePBuffers:
                state->PendingWritePBuffer = false;
                break;
            case EHeldKind::Erase:
                state->PendingErase = false;
                break;
            case EHeldKind::Sync:
                state->PendingSync = false;
                break;
        }
    }

    for (auto& request: held) {
        switch (request.Kind) {
            case EHeldKind::Write:
            case EHeldKind::WritePBuffer:
            case EHeldKind::WritePBuffers: {
                auto guard = Guard(state->Lock);
                state->Payloads[request.Key] = request.Payload;
                break;
            }
            default:
                break;
        }
    }

    for (auto& request: held) {
        switch (request.Kind) {
            case EHeldKind::Read: {
                TRope data;
                {
                    auto guard = Guard(state->Lock);
                    if (const auto* payload =
                            state->Payloads.FindPtr(request.Key))
                    {
                        data = *payload;
                    }
                }
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvReadResult(
                        TReplyStatus::OK,
                        std::nullopt,
                        std::move(data)),
                    0,
                    request.Cookie));
                break;
            }
            case EHeldKind::ReadPBuffer: {
                TRope data;
                {
                    auto guard = Guard(state->Lock);
                    if (const auto* payload =
                            state->Payloads.FindPtr(request.Key))
                    {
                        data = *payload;
                    }
                }
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvReadPersistentBufferResult(
                        TReplyStatus::OK,
                        std::nullopt,
                        request.Key.VChunkIndex,
                        request.Key.OffsetInBytes,
                        data.size(),
                        std::move(data)),
                    0,
                    request.Cookie));
                break;
            }
            case EHeldKind::Write:
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvWriteResult(TReplyStatus::OK),
                    0,
                    request.Cookie));
                break;
            case EHeldKind::WritePBuffer:
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvWritePersistentBufferResult(
                        TReplyStatus::OK),
                    0,
                    request.Cookie));
                break;
            case EHeldKind::WritePBuffers: {
                bool split = false;
                {
                    auto guard = Guard(state->Lock);
                    split = state->SplitWriteToManyReplies &&
                            request.PersistentBufferIds.size() > 1;
                }
                auto sendSlice = [&](size_t begin, size_t end)
                {
                    auto response = std::make_unique<
                        NDDisk::TEvWritePersistentBuffersResult>();
                    for (size_t i = begin; i < end; ++i) {
                        auto* res = response->Record.AddResult();
                        *res->MutablePersistentBufferId() =
                            request.PersistentBufferIds[i];
                        res->MutableResult()->SetStatus(TReplyStatus::OK);
                    }
                    actorSystem->Send(new IEventHandle(
                        request.Sender,
                        TActorId(),
                        response.release(),
                        0,
                        request.Cookie));
                };
                if (split) {
                    const size_t mid = request.PersistentBufferIds.size() / 2;
                    sendSlice(0, mid);
                    sendSlice(mid, request.PersistentBufferIds.size());
                } else {
                    sendSlice(0, request.PersistentBufferIds.size());
                }
                break;
            }
            case EHeldKind::Erase:
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvErasePersistentBufferResult(
                        TReplyStatus::OK),
                    0,
                    request.Cookie));
                break;
            case EHeldKind::Sync:
                actorSystem->Send(new IEventHandle(
                    request.Sender,
                    TActorId(),
                    new NDDisk::TEvSyncResult(TReplyStatus::OK),
                    0,
                    request.Cookie));
                break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
