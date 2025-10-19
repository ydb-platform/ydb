#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NMLP {

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

struct TCommitterSettings {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    // TODO check access
};

IActor* CreateCommitter(const NActors::TActorId& parentId, TCommitterSettings&& settings);

struct TUnlockerSetting {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;

    // TODO check access
};

IActor* CreateUnlocker(const NActors::TActorId& parentId, TUnlockerSetting&& settings);

struct TMessageDeadlineChangerSetting {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    std::vector<TMessageId> Messages;
    TInstant Deadline;

    // TODO check access
};

IActor* CreateMessageDeadlineChanger(const NActors::TActorId& parentId, TMessageDeadlineChangerSetting&& settings);

} // NKikimr::NPQ::NMLP
