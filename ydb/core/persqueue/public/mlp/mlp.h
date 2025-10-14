#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NPQ::NMLP {

struct TReaderSetting {
    TString DatabasePath;
    TString TopicName;
    TString Consumer;
    TDuration WaitTime = TDuration::Zero();
    TDuration VisibilityTimeout = TDuration::Seconds(30);
    ui32 MaxNumberOfMessage = 1;

    // TODO check access
};

// Reply TEvPersQueue::TEvMLPReadResponse
IActor* CreateReader(const NActors::TActorId& parentId, TReaderSetting&& settings);


} // NKikimr::NPQ::NMLP
