#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ::NCloudEvents {

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

/** Serialized cloud event payload (protobuf wire format); name kept for API compatibility. */
TString GetCloudEventType(const TCloudEventInfo& info);

NActors::IActor* CreateCloudEventActor();

} // namespace NKikimr::NPQ::NCloudEvents
