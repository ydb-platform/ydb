#pragma once

#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DestroyErrorMessage =
    "TICStorageTransportActor is destroyed";
constexpr TStringBuf CantAcquireDataErrorMessage = "can't acquire data";
constexpr TStringBuf UndeliveryErrorMessage = "Undelivered";

////////////////////////////////////////////////////////////////////////////////

enum class ETranslateFlags
{
    None,
    TreatOutdatedAsSuccess,
};

NProto::TError TranslateError(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse,
    const TString& errorReason,
    ETranslateFlags flags = ETranslateFlags::None);

template <typename T>
NProto::TError TranslateError(
    const T& response,
    ETranslateFlags flags = ETranslateFlags::None)
{
    return TranslateError(
        response.GetStatus(),
        response.GetErrorReason(),
        flags);
}

////////////////////////////////////////////////////////////////////////////////

bool HasSuccess(NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse);

template <typename T>
bool HasSuccess(const T& response)
{
    return HasSuccess(response.GetStatus());
}

////////////////////////////////////////////////////////////////////////////////

bool HasSuccessOrOutdated(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse);

template <typename T>
bool HasSuccessOrOutdated(const T& response)
{
    return HasSuccessOrOutdated(response.GetStatus());
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SetCantAcquireStatus(T& record)
{
    record.SetStatus(NKikimrBlobStorage::NDDisk::TReplyStatus::UNKNOWN);
    record.SetErrorReason(TString(CantAcquireDataErrorMessage));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
