#include "cloud_events.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <grpcpp/support/status.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillAuthentication() {
        Ev.mutable_authentication()->set_authenticated(true);
        Ev.mutable_authentication()->set_subject_id(EventInfo.UserSID);
        Ev.mutable_authentication()->set_subject_type(EventInfo.AuthType);
        if (!EventInfo.UserSanitizedToken.empty()) {
            Ev.mutable_authentication()->mutable_token_info->set_masked_iam_token(EventInfo.UserSanitizedToken);
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillAuthorization() {
        // authorized = true;
        // permissions.permission: см. permissions из SearchEvents;
        // permissions.resource_type = resource_type из SearchEvents;
        // permissions.resource_id = resource_id из SearchEvents;
        // permissions.authorized оставляем пустым.

        Ev.mutable_authorization()->set_authorized(true);
        Ev.mutable_authorization()->mutable_permissions()->permission(EventInfo.Permission);
        Ev.mutable_authorization()->mutable_permissions()->resource_type(EventInfo.ResourceType);
        Ev.mutable_authorization()->mutable_permissions()->resource_id(EventInfo.FolderId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillEventMetadata() {
        // event_id: судя по всему, какого-либо четкого формата у этого поля нет, главное - чтобы он был глобально уникальным, предлагается event_type + "$" + guid;
        // event_type = "yandex.cloud.events.yq.CreateMessageQueue";
        // created_at = timestamp из SearchEvent;
        // tracing_context оставляем пустым
        // cloud_id = cloud_id из SearchEvent;
        // folder_id = folder_id из SearchEvent.

        Ev.mutable_event_metadata()->set_event_id(EventInfo.Id);
        Ev.mutable_event_metadata()->set_event_type(EventInfo.Type);
        Ev.mutable_event_metadata()->set_created_at(EventInfo.Timestamp);
        Ev.mutable_event_metadata()->set_cloud_id(EventInfo.CloudId);
        Ev.mutable_event_metadata()->set_folder_id(EventInfo.FolderId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillRequestMetadata() {
        // remote_address - ip-адрес клиента (но это лишь в абстрактном "идеале", в реальности - мы в лучшем случае можем записать туда того, кто дернул YDB'шную grpc-ручку);
        // remote_port - про то же, причем не всегда возможно туда поставить хоть что-то;
        // request_id - поле обязательно для заполнения, даже если действие не было вызвано обращением к API; в этом случае следует самостоятельно генерировать уникальный request_id, одинаковый для всех событий серии;
        // idempotency_id - имеется ввиду некий индикатор того, сколько раз было послано одно и то же сообщение с одним и тем же смыслом.

        Ev.mutable_request_metadata()->set_remote_address(EventInfo.RemoteAddress);
        Ev.mutable_request_metadata()->set_remote_port(EventInfo.RemotePort);
        Ev.mutable_request_metadata()->set_request_id(EventInfo.RequestId);
        Ev.mutable_request_metadata()->set_idempotency_id(EventInfo.IdempotencyId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillStatus() {
        if (EventInfo.Issue.empty()) {
            Ev.set_event_status(EStatus::DONE);
        } else {
            Ev.set_event_status(EStatus::ERROR);
            Ev.mutable_error()->set_code(grpc::StatusCode::UNKNOWN);
            Ev.mutable_error()->set_error(EventInfo.Issue);
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillDetails() {
        Ev.mutable_details()->set_name(EventInfo.Name);
        Ev.mutable_details()->set_labels(EventInfo.Labels);

        // {
        //     NJson::TJsonValue json;
        //     NJson::ReadJsonTree(EventInfo.Labels, &json);
        //     for (const auto& [key, value] : json.GetMap()) {
        //         Ev.mutable_details()->mutable_labels()[key] = value;
        //     }
        // }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::Fill() {
        FillAuthentication();
        FillAuthorization();
        FillEventMetadata();
        FillRequestMetadata();
        FillStatus();
        FillDetails();
    }

// ===============================================================

    template<typename TProtoEvent>
    void TAuditSender<TProtoEvent>::Send(const TProtoEvent& ev) {
        Y_UNUSED(ev);
    }

// ===============================================================

    constexpr std::string_view TProcessor::GetSelectQuery() const {
        return R"(
            select...
        )";
    }

    constexpr std::string_view TProcessor::GetDeleteQuery() const {
        return R"(
            delete...
        )";
    }

    void TProcessor::RunQuery(std::string_view query) {
        Y_UNUSED(query); // remove it
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();
        Y_UNUSED(request); // remove it

        // ...

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release(), IEventHandle::FlagTrackDelivery, ++LastCookie);
    }

    void TProcessor::Bootstrap() {
        Become(&TProcessor::StateWaitWakeUp);
    }

    void TProcessor::HandleRetry(const NActors::TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Cookie == LastCookie) {
            RunQuery(LastQuery);
        }
    }

    void TProcessor::HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&) {
        LastQuery = GetSelectQuery();
        RunQuery(LastQuery);
        Become(&TProcessor::StateWaitSelectResponse);
    }

    void TProcessor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Y_UNUSED(ev); // remove it
        // if (delete...) {
        //     return;
        // }

        // **

        LastQuery = GetDeleteQuery();
        RunQuery(LastQuery);
        Become(&TProcessor::StateWaitDeleteResponse);
    }

    void TProcessor::HandleDeleteResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Y_UNUSED(ev); // remove it
        // if (select...) {
        //     return;
        // }

        // TAuditSender::Send()...

        Schedule(DefaultRetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }
} // namespace NCloudEvents
} // namespace NKikimr::NSQS
