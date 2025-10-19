#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NMLP {

enum EEv : ui32 {
    EvChangeResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::MLP),
    EvEnd
};

struct TMessageId {
    ui32 PartitionId;
    ui64 Offset;
};

struct TReaderSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    TDuration WaitTime = TDuration::Zero();
    TDuration VisibilityTimeout = TDuration::Seconds(30);
    ui32 MaxNumberOfMessage = 1;

    // TODO check access
};

// Reply TEvPersQueue::TEvMLPReadResponse or TEvPersQueue::TEvMLPErrorResponse 
IActor* CreateReader(const NActors::TActorId& parentId, TReaderSettings&& settings);

struct TEvChangeResponse : public NActors::TEventLocal<TEvChangeResponse, EEv::EvChangeResponse> {

    TEvChangeResponse(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, TString&& errorDescription = {})
        : Status(status)
        , ErrorDescription(std::move(errorDescription))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorDescription;

    struct TResult {
        TMessageId MessageId;
        bool Success = false;
    };
    // The original topic path (from request) -> TopicInfo
    std::vector<TResult> Messages;
};


struct TCommitterSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    // TODO check access
};

// Return TEvChangeResponse
IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings);

struct TUnlockerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    // TODO check access
};

// Return TEvChangeResponse
IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSettings&& settings);

struct TMessageDeadlineChangerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;
    TInstant Deadline;

    // TODO check access
};

// Return TEvChangeResponse
IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSettings&& settings);

} // NKikimr::NPQ::NMLP
