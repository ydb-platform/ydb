#include "error_utils.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

NProto::TError TranslateError(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse,
    const TString& errorReason,
    ETranslateFlags flags)
{
    switch (flags) {
        case ETranslateFlags::None: {
            if (HasSuccess(errorResponse)) {
                return MakeError(S_OK);
            }
            break;
        }
        case ETranslateFlags::TreatOutdatedAsSuccess: {
            if (HasSuccessOrOutdated(errorResponse)) {
                return MakeError(S_OK);
            }
            break;
        }
    }

    return MakeError(
        E_FAIL,
        TReplyStatus_E_Name(errorResponse) + " " + errorReason);
}

bool HasSuccess(NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse)
{
    return errorResponse == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
}

bool HasSuccessOrOutdated(
    NKikimrBlobStorage::NDDisk::TReplyStatus_E errorResponse)
{
    return errorResponse == NKikimrBlobStorage::NDDisk::TReplyStatus::OK ||
           errorResponse == NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
