#include "cloud_events.h"

namespace NKikimr::NSQS {
namespace NCloudEvents {
    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillAuthentication() {
        Ev.mutable_authentication()->set_authenticated(true);
        Ev.mutable_authentication()->set_subject_id(NAuditHelpers::MaybeRemoveSuffix(EventInfo.UserToken.GetUserSID()));
        Ev.mutable_authentication()->set_subject_type(NAuditHelpers::GetCloudSubjectType(EventInfo.AuthType)); // TODO: look at fq/control_plane_proxy.cpp
        if (EventInfo.UserToken.HasSanitizedToken()) {
            Ev.mutable_authentication()->mutable_token_info->set_masked_iam_token(EventInfo.UserToken.GetSanitizedToken());
        }
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillAuthorization() {
        // authorized = true;
        // permissions.permission: см. permissions из SearchEvents;
        // permissions.resource_type = resource_type из SearchEvents;
        // permissions.resource_id = resource_id из SearchEvents;
        // permissions.authorized оставляем пустым.

        // Ev.mutable_authorization()->set_authorized(true);
        // Ev.mutable_authorization()->mutable_permissions()->permission();
        // Ev.mutable_authorization()->mutable_permissions()->resource_type();
        // Ev.mutable_authorization()->mutable_permissions()->resource_id();
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillEventMetadata() {
        // event_id: судя по всему, какого-либо четкого формата у этого поля нет, главное - чтобы он был глобально уникальным, предлагается event_type + "$" + guid;
        // event_type = "yandex.cloud.events.yq.CreateMessageQueue";
        // created_at = timestamp из SearchEvent;
        // tracing_context оставляем пустым
        // cloud_id = cloud_id из SearchEvent;
        // folder_id = folder_id из SearchEvent.

        // Ev.mutable_event_metadata()->set_event_id();
        // Ev.mutable_event_metadata()->set_created_at();
        // Ev.mutable_event_metadata()->set_cloud_id();
        // Ev.mutable_event_metadata()->set_folder_id();
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillRequestMetadata() {
        // remote_address - ip-адрес клиента (но это лишь в абстрактном "идеале", в реальности - мы в лучшем случае можем записать туда того, кто дернул YDB'шную grpc-ручку);
        // remote_port - про то же, причем не всегда возможно туда поставить хоть что-то;
        // request_id - поле обязательно для заполнения, даже если действие не было вызвано обращением к API; в этом случае следует самостоятельно генерировать уникальный request_id, одинаковый для всех событий серии;
        // idempotency_id - имеется ввиду некий индикатор того, сколько раз было послано одно и то же сообщение с одним и тем же смыслом.

        // Ev.mutable_request_metadata()->set_remote_address();
        // Ev.mutable_request_metadata()->set_remote_port();
        // Ev.mutable_request_metadata()->set_request_id();
        // Ev.mutable_request_metadata()->set_idempotency_id();
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillEventStatus() {
        // enum EventStatus {
        //     EVENT_STATUS_UNSPECIFIED = 0;
        //     STARTED = 1;
        //     ERROR = 2;
        //     DONE = 3;
        //     CANCELLED = 4;
        // }

        // Ev.set_event_status();
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::FillDetails() {
        // map<string, string> labels = 1;
        // repeated string name = 2;

        // Ev.mutable_details()->set_name();
        // Ev.mutable_details()->set_labels();
    }

    template<typename TProtoEvent>
    void TFillerBase<TProtoEvent>::Fill() {
        // FillAuthentication();
        // FillAuthorization();
        // FillEventMetadata();
        // FillRequestMetadata();
        // FillEventStatus();
        // FillDetails();
    }

    void TFillerCreateQueue::Fill() {
        TFillerBase::Fill();

        Ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.yq.CreateMessageQueue");
    }

    void TFillerUpdateQueue::Fill() {
        TFillerBase::Fill();

        Ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.yq.UpdateMessageQueue");
    }

    void TFillerDeleteQueue::Fill() {
        TFillerBase::Fill();

        Ev.mutable_event_metadata()->set_event_type("yandex.cloud.events.yq.DeleteMessageQueue");
    }

// ===============================================================

    template<typename TProtoEvent>
    void TSender<TProtoEvent>::Send(const TProtoEvent& ev) {
        Y_UNUSED(ev);
    }

// ===============================================================

    void TProcessor::Bootstrap() {

    }

    void TProcessor::HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr& ev) {
        // State machine
        Y_UNUSED(ev);
    }

    void TProcessor::HandleQueryResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        // State machine
        Y_UNUSED(ev);
    }
} // namespace NCloudEvents
} // namespace NKikimr::NSQS
