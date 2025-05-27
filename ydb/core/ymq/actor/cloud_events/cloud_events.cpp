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
        // Ev.mutable_authentication()->set_subject_type(EventInfo.AuthType); TODO
        if (!EventInfo.UserSanitizedToken.empty()) {
            // Ev.mutable_authentication()->mutable_token_info()->set_masked_iam_token(EventInfo.UserSanitizedToken); // TODO compile
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
        // Ev.mutable_authorization()->mutable_permissions()->set_permission(EventInfo.Permission); // TODO compile
        // Ev.mutable_authorization()->mutable_permissions()->set_resource_type(EventInfo.ResourceType); // TODO compile
        // Ev.mutable_authorization()->mutable_permissions()->set_resource_id(EventInfo.FolderId); // TODO compile
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillEventMetadata() {
        // event_id: судя по всему, какого-либо четкого формата у этого поля нет, главное - чтобы он был глобально уникальным, предлагается event_type + "$" + guid;
        // event_type = "yandex.cloud.events.ymq.CreateMessageQueue";
        // created_at = timestamp из SearchEvent;
        // tracing_context оставляем пустым
        // cloud_id = cloud_id из SearchEvent;
        // folder_id = folder_id из SearchEvent.

        Ev.mutable_event_metadata()->set_event_id(EventInfo.Id);
        Ev.mutable_event_metadata()->set_event_type(EventInfo.Type);
        Ev.mutable_event_metadata()->mutable_created_at()->set_seconds(EventInfo.CreatedAt);
        Ev.mutable_event_metadata()->set_cloud_id(EventInfo.CloudId);
        Ev.mutable_event_metadata()->set_folder_id(EventInfo.FolderId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillRequestMetadata() {
        // remote_address - ip-адрес клиента (но это лишь в абстрактном "идеале", в реальности - мы в лучшем случае можем записать туда того, кто дернул YDB'шную grpc-ручку);
        // request_id - поле обязательно для заполнения, даже если действие не было вызвано обращением к API; в этом случае следует самостоятельно генерировать уникальный request_id, одинаковый для всех событий серии;
        // idempotency_id - имеется ввиду некий индикатор того, сколько раз было послано одно и то же сообщение с одним и тем же смыслом.

        Ev.mutable_request_metadata()->set_remote_address(EventInfo.RemoteAddress);
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
            // Ev.mutable_error()->set_error(EventInfo.Issue);
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillDetails() {
        Ev.mutable_details()->set_name(EventInfo.QueueName);

        for (const auto& [k, jsonValue] : EventInfo.Labels) {
            Ev.mutable_details()->mutable_labels()->insert({TString(k), jsonValue.GetStringRobust()});
        }
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
    void TAuditSender::Send(const TProtoEvent& ev) {
        std::cerr << "cloud_events.cpp: I got the audit message!!!" << std::endl;
        std::cerr << "cloud_events.cpp: auditsender: type: " << ev.event_metadata().event_type() << std::endl;
    }

    template void TAuditSender::Send<TCreateQueueEvent>(const TCreateQueueEvent&);
    template void TAuditSender::Send<TUpdateQueueEvent>(const TUpdateQueueEvent&);
    template void TAuditSender::Send<TDeleteQueueEvent>(const TDeleteQueueEvent&);

// ===============================================================
    TString TProcessor::GetFullTablePath() const {
        return TStringBuilder() << Root << "/" << EventTableName;
    }

    TString TProcessor::GetInitSelectQuery() const {
        return TStringBuilder()
            << "--!syntax_v1" << "\n"
            << "SELECT" << "\n"
            << "Id,"
            << "QueueName,"
            << "CreatedAt,"
            << "Type,"
            << "CloudId,"
            << "FolderId,"
            << "UserSID,"
            << "UserSanitizedToken,"
            << "AuthType,"
            << "PeerName,"
            << "RequestId,"
            << "IdempotencyId,"
            << "Labels" << "\n"
            << "FROM `" << GetFullTablePath() << "`;\n";
    }

    TString TProcessor::GetInitDeleteQuery() const {
        return TStringBuilder()
            << "--!syntax_v1" << "\n"
            << "DECLARE $Events AS List<Struct<Id:Uint64, QueueName:Utf8>>;" << "\n"
            << "$EventsSource = (SELECT item.Id AS Id, item.QueueName AS QueueName" << "\n"
                             << "FROM (SELECT $Events AS events) FLATTEN LIST BY events as item);" << "\n"
            << "DELETE FROM `" << GetFullTablePath() << "`" << "\n"
            << "ON SELECT * FROM $EventsSource;" << "\n";
    }

    TProcessor::TProcessor
    (
        TString root,
        TString database
    )
        : Root(root)
        , Database(database)
        , SelectQuery(GetInitSelectQuery())
        , DeleteQuery(GetInitDeleteQuery())
    {
    }

    void TProcessor::RunQuery(TString query, std::unique_ptr<NYdb::TParams> params, bool readOnly) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();

        request->SetKeepSession(true);

        if (!SessionId.empty()) {
            request->SetSessionId(SessionId);
        }

        if (!Database.empty()) {
            request->SetDatabase(Database);
        }

        request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->SetQuery(query);

        request->MutableQueryCachePolicy()->set_keep_in_cache(true);

        if (readOnly) {
            request->MutableTxControl()->mutable_begin_tx()->mutable_online_read_only();
        } else {
            request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        }

        request->MutableTxControl()->set_commit_tx(true);

        if (params) {
            request->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(*params)));
        }

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release(), IEventHandle::FlagTrackDelivery);
    }

    void TProcessor::Bootstrap() {
        Schedule(DefaultRetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }

    void TProcessor::StopSession() {
        if (!SessionId.empty()) {
            auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
            SessionId = TString();
        }
    }

    void TProcessor::ProcessFailure() {
        StopSession();
        Schedule(DefaultRetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }

    void TProcessor::HandleUndelivered(const NActors::TEvents::TEvUndelivered::TPtr&) {
        ProcessFailure();
    }

    void TProcessor::HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&) {
        LastQuery = ELastQueryType::Select;
        RunQuery(SelectQuery);
        Become(&TProcessor::StateWaitSelectResponse);
    }

    std::vector<TEventInfo> TProcessor::ConvertSelectResponseToEventList(const ::NKikimrKqp::TQueryResponse& response) {
        std::vector<TEventInfo> result;

        Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
        NYdb::TResultSetParser parser(response.GetYdbResults(0));

        auto convertLabels = [](const TString& str) -> THashMap<TBasicString<char>, NJson::TJsonValue> {
            NJson::TJsonValue json;
            NJson::ReadJsonTree(str, &json);
            return json.GetMap();
        };

        auto convertId = [](uint_fast64_t id, const TString& type, uint_fast64_t createdAt) -> TString {
            return TStringBuilder() << id << "$" << type << "$" << createdAt;
        };

        while (parser.TryNextRow()) {
            result.push_back({});
            auto& cloudEvent = result.back();

            cloudEvent.OriginalId = *parser.ColumnParser(0).GetOptionalUint64();
            TString type = *parser.ColumnParser(1).GetOptionalUtf8();
            cloudEvent.Type = DefaultEventTypePrefix + type;
            cloudEvent.CreatedAt = *parser.ColumnParser(2).GetOptionalUint64();

            cloudEvent.Id = convertId(cloudEvent.OriginalId, cloudEvent.Type, cloudEvent.CreatedAt);

            cloudEvent.CloudId = *parser.ColumnParser(3).GetOptionalUtf8();
            cloudEvent.FolderId = *parser.ColumnParser(4).GetOptionalUtf8();
            cloudEvent.UserSID = *parser.ColumnParser(5).GetOptionalUtf8();
            cloudEvent.UserSanitizedToken = *parser.ColumnParser(6).GetOptionalUtf8();
            cloudEvent.AuthType = *parser.ColumnParser(7).GetOptionalUtf8();
            cloudEvent.RemoteAddress = *parser.ColumnParser(8).GetOptionalUtf8();
            cloudEvent.RequestId = *parser.ColumnParser(9).GetOptionalUtf8();
            cloudEvent.IdempotencyId = *parser.ColumnParser(10).GetOptionalUtf8();
            cloudEvent.QueueName = *parser.ColumnParser(11).GetOptionalUtf8();
            cloudEvent.Labels = convertLabels(*parser.ColumnParser(12).GetOptionalUtf8());
        }

        return result;
    }

    void TProcessor::UpdateSessionId(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (SessionId.empty()) {
            SessionId = ev->Get()->Record.GetResponse().GetSessionId();
        } else {
            Y_ABORT_UNLESS(SessionId == ev->Get()->Record.GetResponse().GetSessionId());
        }
    }

    void TProcessor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            TString error = "DeleteResponse: Failed.\n";
            for (const auto& issue : record.GetResponse().GetQueryIssues()) {
                error += issue.message() + "\n";
            }

            std::cerr << error << std::endl;
            // LOG_ERROR_S(this->ActorContext(), NKikimrServices::SQS, error);

            ProcessFailure();
            return;
        }

        const auto& response = record.GetResponse();

        EventsList = ConvertSelectResponseToEventList(response);

        UpdateSessionId(ev);

        LastQuery = ELastQueryType::Delete;

        NYdb::TParamsBuilder paramsBuilder;

        auto& param = paramsBuilder.AddParam("$Events");
        param.BeginList();

        for (const auto& cloudEv : EventsList) {
            param.AddListItem()
                .BeginStruct()
                .AddMember("Id")
                    .Uint64(cloudEv.OriginalId)
                .AddMember("QueueName")
                    .Utf8(cloudEv.QueueName)
                .EndStruct();
        }

        param.EndList();
        param.Build();

        auto params = paramsBuilder.Build();

        RunQuery(DeleteQuery, std::make_unique<decltype(params)>(params), false);
        Become(&TProcessor::StateWaitDeleteResponse);
    }

    void TProcessor::HandleDeleteResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            TString error = "DeleteResponse: Failed.\n";
            for (const auto& issue : record.GetResponse().GetQueryIssues()) {
                error += issue.message() + "\n";
            }

            std::cerr << error << std::endl;
            // LOG_ERROR_S(this->ActorContext(), NKikimrServices::SQS, error);

            ProcessFailure();
            return;
        }

        UpdateSessionId(ev);

        for (const auto& cloudEvent : EventsList) {
            std::string_view typeView(cloudEvent.Type.begin() + DefaultEventTypePrefix.size(), cloudEvent.Type.end());

            if (typeView == "CreateMessageQueue") {
                TCreateQueueEvent ev;
                TFiller<TCreateQueueEvent> filler(cloudEvent, ev);
                filler.Fill();
                TAuditSender::Send<TCreateQueueEvent>(ev);
            } else if (typeView == "UpdateMessageQueue") {
                TUpdateQueueEvent ev;
                TFiller<TUpdateQueueEvent> filler(cloudEvent, ev);
                filler.Fill();
                TAuditSender::Send<TUpdateQueueEvent>(ev);
            } else if (typeView == "DeleteMessageQueue") {
                TDeleteQueueEvent ev;
                TFiller<TDeleteQueueEvent> filler(cloudEvent, ev);
                filler.Fill();
                TAuditSender::Send<TDeleteQueueEvent>(ev);
            } else {
                // Y_UNREACHABLE();
            }
        }

        LastQuery = ELastQueryType::None;
        Schedule(DefaultRetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }
} // namespace NCloudEvents
} // namespace NKikimr::NSQS
