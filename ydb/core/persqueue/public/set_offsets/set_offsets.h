#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/persqueue/events/internal/protos/events.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <vector>

namespace NKikimr::NPQ::NSetOffsets {

enum EEv : ui32 {
    EvSetOffsetsResult = InternalEventSpaceBegin(NPQ::NEvents::EServices::SET_OFFSETS),
    EvEnd
};

struct TSetOffsetsSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    NKikimrPQ::TEvSetOffsetsRequest::EPosition Position = NKikimrPQ::TEvSetOffsetsRequest::POSITION_UNSPECIFIED;
    ui64 TimestampMs = 0;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

struct TPartitionResult {
    ui32 PartitionId = 0;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::GENERIC_ERROR;
    TString Error;
};

struct TEvSetOffsetsResult : public NActors::TEventLocal<TEvSetOffsetsResult, EEv::EvSetOffsetsResult> {
    TEvSetOffsetsResult(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString error = {},
        std::vector<TPartitionResult> partitions = {})
        : Status(status)
        , Error(std::move(error))
        , Partitions(std::move(partitions))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString Error;
    std::vector<TPartitionResult> Partitions;
};

NActors::IActor* CreateSetOffsetsActor(const NActors::TActorId& parentId, TSetOffsetsSettings&& settings);

} // namespace NKikimr::NPQ::NSetOffsets
