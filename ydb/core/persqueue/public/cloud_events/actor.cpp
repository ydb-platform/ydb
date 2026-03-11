#include "actor.h"

#include <google/protobuf/json/json.h>
#include <ydb/core/audit/audit_log.h>
#include <google/protobuf/util/time_util.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NPQ::NCloudEvents {

namespace {

static void Fill(TCreateTopicEvent& ev, const TCloudEventInfo& info) {
    // Authentication
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    // TODO: подобрать корректный subject_type по AuthType
    // ev.mutable_authentication()->set_subject_type(...);
    if (!info.MaskedToken.empty()) {
        ev.mutable_authentication()
            ->mutable_token_info()
            ->set_masked_iam_token(info.MaskedToken);
    }

    // Authorization
    ev.mutable_authorization()->set_authorized(true);

    // EventMetadata
    ev.mutable_event_metadata()->set_event_id(info.OperationType + ":" + info.TopicPath);
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + info.OperationType);
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    // RequestMetadata
    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_request_id(info.RequestId);

    // Status
    if (info.Issue.empty()) {
        ev.set_event_status(EStatus::DONE);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    ev.mutable_details()->set_path(info.TopicPath);
}

static void Fill(TAlterTopicEvent& ev, const TCloudEventInfo& info) {
    // Можно вынести в шаблон, здесь дублирую для простоты
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    if (!info.MaskedToken.empty()) {
        ev.mutable_authentication()
            ->mutable_token_info()
            ->set_masked_iam_token(info.MaskedToken);
    }
    ev.mutable_authorization()->set_authorized(true);

    ev.mutable_event_metadata()->set_event_id(info.OperationType + ":" + info.TopicPath);
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + info.OperationType);
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_request_id(info.RequestId);

    if (info.Issue.empty()) {
        ev.set_event_status(EStatus::DONE);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    ev.mutable_details()->set_path(info.TopicPath);
}

static void Fill(TDeleteTopicEvent& ev, const TCloudEventInfo& info) {
    ev.mutable_authentication()->set_authenticated(true);
    ev.mutable_authentication()->set_subject_id(info.UserSID);
    if (!info.MaskedToken.empty()) {
        ev.mutable_authentication()
            ->mutable_token_info()
            ->set_masked_iam_token(info.MaskedToken);
    }
    ev.mutable_authorization()->set_authorized(true);

    ev.mutable_event_metadata()->set_event_id(info.OperationType + ":" + info.TopicPath);
    ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.ydb.topics." + info.OperationType);
    auto ts = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(info.CreatedAt.MilliSeconds());
    *ev.mutable_event_metadata()->mutable_created_at() = ts;
    ev.mutable_event_metadata()->set_cloud_id(info.CloudId);
    ev.mutable_event_metadata()->set_folder_id(info.FolderId);

    ev.mutable_request_metadata()->set_remote_address(info.RemoteAddress);
    ev.mutable_request_metadata()->set_request_id(info.RequestId);

    if (info.Issue.empty()) {
        ev.set_event_status(EStatus::DONE);
    } else {
        ev.set_event_status(EStatus::ERROR);
        ev.mutable_error()->set_message(info.Issue);
    }

    ev.mutable_details()->set_path(info.TopicPath);
}

template<typename TEvent>
TString SerializeEvent(const TEvent& ev) {
    TString json;
    google::protobuf::json::PrintOptions printOpts;
    printOpts.preserve_proto_field_names = true;
    printOpts.always_print_primitive_fields = true;
    google::protobuf::json::MessageToJsonString(ev, &json, printOpts);
    return json;
}

}

void TCloudEventsActor::Bootstrap() {
    Become(&TCloudEventsActor::StateWork);
}

void TCloudEventsActor::Handle(TCloudEvent::TPtr& ev) {
    TString json;

    auto type = ev.Get()->Get()->Info.ModifyScheme.GetOperationType();
    if (type == NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup) {
        TCreateTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup) {
        TAlterTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    } else if (type == NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup) {
        TDeleteTopicEvent proto;
        Fill(proto, ev.Get()->Get()->Info);
        json = SerializeEvent(proto);
    }

    AUDIT_LOG(
        AUDIT_PART("cloud_event_json", json)
    );
}

} // namespace NKikimr::NPQ::NCloudEvents