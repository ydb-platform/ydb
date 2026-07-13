#include "ddisk_stub_actor.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

using namespace NActors;
using namespace NKikimr;

namespace {

using TReplyStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;

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
    {
        auto guard = Guard(State->Lock);
        if (State->PendingRead) {
            return;
        }
    }

    auto response = std::make_unique<NDDisk::TEvReadResult>(TReplyStatus::OK);
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDDiskStubActor::HandleWrite(
    const NDDisk::TEvWrite::TPtr& ev,
    const TActorContext& ctx)
{
    {
        auto guard = Guard(State->Lock);
        if (State->PendingWrite) {
            return;
        }
    }

    auto response = std::make_unique<NDDisk::TEvWriteResult>(TReplyStatus::OK);
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
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
        HFunc(NDDisk::TEvListPersistentBuffer, HandleListPersistentBuffer);

        default:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
