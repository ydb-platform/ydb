#pragma once

#include "error.h"

#include <ydb/core/protos/sqs.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/services/datastreams/codes/datastreams_codes.h>

namespace NKikimr::NSqsTopic::V1 {
    struct TEvSqsTopic {
        enum EEv {
            EvPartitionActorResult,
        };

        struct TEvPartitionActorResult: public NActors::TEventLocal<TEvPartitionActorResult, EvPartitionActorResult> {
            struct TMessage {
                i64 Offset;
                TString Id;
                ui32 BatchIndex;

                TMaybe<NSQS::TError> Error;
            };

            ui32 PartitionId;
            TVector<TMessage> Messages;
        };
    };

} // namespace NKikimr::NSqsTopic::V1
