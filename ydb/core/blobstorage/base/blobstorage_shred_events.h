#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    struct TEvBlobStorage::TEvControllerShredRequest : TEventPB<TEvControllerShredRequest,
            NKikimrBlobStorage::TEvControllerShredRequest, TEvBlobStorage::EvControllerShredRequest> {
        TEvControllerShredRequest() = default;

        TEvControllerShredRequest(ui64 generation) {
            Record.SetGeneration(generation);
        }
    };

    struct TEvBlobStorage::TEvControllerShredResponse : TEventPB<TEvControllerShredResponse,
            NKikimrBlobStorage::TEvControllerShredResponse, TEvBlobStorage::EvControllerShredResponse> {
        TEvControllerShredResponse() = default;

        TEvControllerShredResponse(ui64 generation, bool completed, ui32 progress) {
            Record.SetCurrentGeneration(generation);
            Record.SetCompleted(completed);
            Record.SetProgress10k(progress);
        }
    };

}
