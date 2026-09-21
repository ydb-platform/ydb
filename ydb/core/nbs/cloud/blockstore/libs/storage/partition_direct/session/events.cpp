#include "events.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TEvPartitionSession::TEvMount::TEvMount(
    TString registrationId,
    TString clientId)
    : RegistrationId(std::move(registrationId))
    , ClientId(std::move(clientId))
    , Result(NThreading::NewPromise<TResultOrError<TString>>())
{}

TEvPartitionSession::TEvMount::~TEvMount()
{
    Result.TrySetValue(MakeError(E_REJECTED, "Partition did not handle mount"));
}

TEvPartitionSession::TEvUnmount::TEvUnmount(
    TString registrationId,
    TString clientId,
    TString sessionId)
    : RegistrationId(std::move(registrationId))
    , ClientId(std::move(clientId))
    , SessionId(std::move(sessionId))
    , Result(NThreading::NewPromise<NProto::TError>())
{}

TEvPartitionSession::TEvUnmount::~TEvUnmount()
{
    Result.TrySetValue(
        MakeError(E_REJECTED, "Partition did not handle unmount"));
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
