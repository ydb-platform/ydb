#include "helpers.h"

#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeKikimrError(NKikimrProto::EReplyStatus status,
                               TString errorReason)
{
    NProto::TError error;

    if (status != NKikimrProto::OK) {
        error.SetCode(MAKE_KIKIMR_ERROR(status));

        if (errorReason) {
            error.SetMessage(std::move(errorReason));
        } else {
            error.SetMessage(NKikimrProto::EReplyStatus_Name(status));
        }
    }

    return error;
}

NProto::TError MakeSchemeShardError(NKikimrScheme::EStatus status,
                                    TString errorReason)
{
    NProto::TError error;

    if (status != NKikimrScheme::StatusSuccess) {
        error.SetCode(MAKE_SCHEMESHARD_ERROR(status));

        if (errorReason) {
            error.SetMessage(std::move(errorReason));
        } else {
            error.SetMessage(NKikimrScheme::EStatus_Name(status));
        }
    }

    return error;
}

}   // namespace NYdb::NBS
