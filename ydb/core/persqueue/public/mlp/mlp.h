#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NACLib {
class TUserToken;
}

namespace NKikimr::NPQ::NMLP {

enum EEv : ui32 {
    EvReadResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::MLP),
    EvChangeResponse,
    EvEnd
};

struct TMessageId {
    ui32 PartitionId;
    ui64 Offset;
};

struct TEvReadResponse : public NActors::TEventLocal<TEvReadResponse, EEv::EvReadResponse> {

    TEvReadResponse(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, TString&& errorDescription = {})
        : Status(status)
        , ErrorDescription(std::move(errorDescription))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorDescription;

    struct TMessage {
        TMessageId MessageId;
        Ydb::Topic::Codec Codec;
        TString Data;
    };
    // The original topic path (from request) -> TopicInfo
    std::vector<TMessage> Messages;
};


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



struct TReaderSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    TDuration WaitTime = TDuration::Zero();
    TDuration VisibilityTimeout = TDuration::Seconds(30);
    ui32 MaxNumberOfMessage = 1;
    bool UncompressMessages = false;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvReadResponse
IActor* CreateReader(const NActors::TActorId& parentId, TReaderSettings&& settings);


struct TCommitterSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvChangeResponse
IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings);

struct TUnlockerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvChangeResponse
IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSettings&& settings);

struct TMessageDeadlineChangerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;
    TInstant Deadline;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvChangeResponse
IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSettings&& settings);

} // NKikimr::NPQ::NMLP
