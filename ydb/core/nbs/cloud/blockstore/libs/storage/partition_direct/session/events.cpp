#include "events.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TEvPartitionSession::TEvMount::TEvMount(TString clientId)
    : ClientId(std::move(clientId))
    , Result(NThreading::NewPromise<TResultOrError<TString>>())
{}

TEvPartitionSession::TEvMount::~TEvMount()
{
    Result.TrySetValue(MakeError(E_REJECTED, "Partition did not handle mount"));
}

TEvPartitionSession::TEvUnmount::TEvUnmount(TString clientId, TString sessionId)
    : ClientId(std::move(clientId))
    , SessionId(std::move(sessionId))
    , Result(NThreading::NewPromise<NProto::TError>())
{}

TEvPartitionSession::TEvUnmount::~TEvUnmount()
{
    Result.TrySetValue(
        MakeError(E_REJECTED, "Partition did not handle unmount"));
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
