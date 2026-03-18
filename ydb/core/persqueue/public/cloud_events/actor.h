#pragma once

#include "events_writer.h"

#include <ydb/core/persqueue/public/cloud_events/proto/topics.pb.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/events.h>
#include <ydb/core/persqueue/events/events.h>

namespace NKikimr::NPQ::NCloudEvents {

using TCreateTopicEvent = yandex::cloud::events::ydb::topics::CreateTopic;
using TAlterTopicEvent = yandex::cloud::events::ydb::topics::AlterTopic;
using TDeleteTopicEvent = yandex::cloud::events::ydb::topics::DeleteTopic;
using EStatus = yandex::cloud::events::EventStatus;

enum class EEv {
    EvCloudEvent = NPQ::NEvents::InternalEventSpaceBegin(NPQ::NEvents::EServices::CLOUD_EVENTS),
};

struct TCloudEventInfo {
    TString CloudId;
    TString FolderId;
    TString DatabaseId;

    TString TopicPath;
    TString Issue;
    TString UserSID;
    TString RemoteAddress;

    TInstant CreatedAt;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
    NKikimrScheme::EStatus OperationStatus;
};

struct TCloudEvent : public NActors::TEventLocal<TCloudEvent, static_cast<ui32>(EEv::EvCloudEvent)> {
    TCloudEventInfo Info;

    explicit TCloudEvent(TCloudEventInfo&& info)
        : Info(std::move(info))
    {}
};

TString BuildTopicCloudEventJson(const TCloudEventInfo& info);
TString GetCloudEventType(const TCloudEventInfo& info);

class TCloudEventsActor : public NActors::TActorBootstrapped<TCloudEventsActor> {
public:
    TCloudEventsActor();
    explicit TCloudEventsActor(IEventsWriter::TPtr eventsWriter);

    void Bootstrap();

private:
    IEventsWriter::TPtr EventsWriter;

    void Handle(TCloudEvent::TPtr& ev);

    STRICT_STFUNC(StateWork,
        hFunc(TCloudEvent, Handle);

        cFunc(TKikimrEvents::TEvPoisonPill::EventType, PassAway);
    )
};

} // namespace NKikimr::NPQ::NCloudEvents