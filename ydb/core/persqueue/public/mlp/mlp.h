#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NACLib {
class TUserToken;
}

namespace NKikimr::NPQ::NMLP {

enum EEv : ui32 {
    EvReadResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::MLP),
    EvWriteResponse,
    EvChangeResponse,
    EvPurgeResponse,
    EvEnd
};

struct TMessageId {
    ui32 PartitionId;
    ui64 Offset;
};

struct TEvWriteResponse : public NActors::TEventLocal<TEvWriteResponse, EEv::EvWriteResponse> {

    NDescriber::EStatus DescribeStatus;

    struct TMessage {
        size_t Index;
        Ydb::StatusIds::StatusCode Status;
        // if message was written successfully, it will be set
        std::optional<TMessageId> MessageId;
    };
    std::vector<TMessage> Messages;
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
        THashMap<TString, TString> MessageMetaAttributes;
        TInstant SentTimestamp;
        TString MessageGroupId;
    };
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
    std::vector<TResult> Messages;
};

struct TEvPurgeResponse : public NActors::TEventLocal<TEvPurgeResponse, EEv::EvPurgeResponse> {

    TEvPurgeResponse(Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, TString&& errorDescription = {})
        : Status(status)
        , ErrorDescription(std::move(errorDescription))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorDescription;
};


struct TWriterSettings {
    TString DatabasePath;
    TString TopicName;

    struct TMessage {
        size_t Index;
        TString MessageBody;
        std::optional<TString> MessageGroupId;
        std::optional<TString> MessageDeduplicationId;
        std::optional<TString> SerializedMessageAttributes;
        TDuration Delay;
    };
    std::vector<TMessage> Messages;

    bool ShouldBeCharged = false;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

IActor* CreateWriter(const NActors::TActorId& parentId, TWriterSettings&& settings);

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
    std::vector<TInstant> Deadlines;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvChangeResponse
IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSettings&& settings);

struct TPurgerSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;

    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

// Return TEvPurgeResponse
IActor* CreatePurger(const NActors::TActorId& parentId, TPurgerSettings&& settings);


} // NKikimr::NPQ::NMLP
