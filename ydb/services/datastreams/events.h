#pragma once

#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NDataStreams::V1 {

    struct TEvDataStreams {
        enum EEv {
            EvPartitionActorResult
        };

        struct TEvPartitionActorResult : public NActors::TEventLocal<TEvPartitionActorResult, EvPartitionActorResult> {
            ui32 PartitionId;
            ui64 CurrentOffset;
            TMaybe<TString> ErrorText;
            TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        };
    };

}

