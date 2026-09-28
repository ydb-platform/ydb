#include "partition_session.h"

#include "partition_session_state.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TPartitionSession::~TPartitionSession() = default;

// static
TResultOrError<TPartitionSessionPtr> TPartitionSession::Create(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
{
    auto state = TPartitionSessionState::Create(
        volumeMetadata,
        std::move(storage),
        std::move(ioGeometry));
    if (HasError(state)) {
        return state.GetError();
    }
    return TPartitionSessionPtr(new TPartitionSession(state.ExtractResult()));
}

const NKikimrBlockStore::TVolumeConfig&
TPartitionSession::GetVolumeMetadata() const
{
    // Every version shares the same immutable metadata, including after Stop.
    return State.AtomicLoad()->GetVolumeMetadata();
}

TString TPartitionSession::GetRegistrationId() const
{
    return State.AtomicLoad()->GetRegistrationId();
}

TResultOrError<TString> TPartitionSession::Mount(const TString& clientId)
{
    auto next = MakeIntrusive<TPartitionSessionState>(*State.AtomicLoad());
    auto result = next->Mount(clientId);
    if (!HasError(result)) {
        State.AtomicStore(next);
    }
    return result;
}

NProto::TError TPartitionSession::Unmount(
    const TString& clientId,
    const TString& sessionId)
{
    auto next = MakeIntrusive<TPartitionSessionState>(*State.AtomicLoad());
    auto result = next->Unmount(clientId, sessionId);
    if (!HasError(result)) {
        State.AtomicStore(next);
    }
    return result;
}

void TPartitionSession::Stop()
{
    auto next = MakeIntrusive<TPartitionSessionState>(*State.AtomicLoad());
    next->Stop();
    State.AtomicStore(next);
}

TResultOrError<TPartitionIoBackend> TPartitionSession::AcquireIoBackend(
    const TString& clientId,
    const TString& sessionId) const
{
    return State.AtomicLoad()->AcquireIoBackend(clientId, sessionId);
}

TPartitionSession::TPartitionSession(
    TIntrusivePtr<TPartitionSessionState> state)
    : State(std::move(state))
{}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
