#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/persqueue/events/internal/protos/events.pb.h>

#include <ydb/library/aclib/aclib.h>

#include <vector>

namespace NKikimr::NPQ::NResetOffset {

enum EEv : ui32 {
    EvResetOffsetResult = InternalEventSpaceBegin(NPQ::NEvents::EServices::RESET_OFFSET),
    EvEnd
};

struct TResetOffsetSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    NKikimrPQ::TEvResetOffsetRequest::EPosition Position = NKikimrPQ::TEvResetOffsetRequest::POSITION_UNSPECIFIED;
    ui64 TimestampMs = 0;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

struct TPartitionResult {
    ui32 PartitionId = 0;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::GENERIC_ERROR;
    TString Error;
};

struct TEvResetOffsetResult : public NActors::TEventLocal<TEvResetOffsetResult, EEv::EvResetOffsetResult> {
    TEvResetOffsetResult(
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

NActors::IActor* CreateResetOffsetActor(const NActors::TActorId& parentId, TResetOffsetSettings&& settings);

} // namespace NKikimr::NPQ::NResetOffset
