#include "cloud_events.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <grpcpp/support/status.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
    void PrintMessageFields(const google::protobuf::Message& message, int indent = 0) {
        const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
        const google::protobuf::Reflection* reflection = message.GetReflection();

        std::string indentation(indent, ' ');

        std::cerr << indentation << "Message type: " << descriptor->full_name() << std::endl;
        std::cerr << indentation << "Fields (" << descriptor->field_count() << "):" << std::endl;

        for (int i = 0; i < descriptor->field_count(); i++) {
            const google::protobuf::FieldDescriptor* field = descriptor->field(i);

            std::cerr << indentation << "  " << i+1 << ". " << field->name()
                    << " (type: " << field->type_name() << ", "
                    << (field->is_repeated() ? "repeated" : "singular") << ")";

            if (field->is_repeated()) {
                int count = reflection->FieldSize(message, field);
                std::cerr << " [" << count << " elements]";

                if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                    std::cerr << std::endl;

                    if (count > 0) {
                        for (int j = 0; j < count; j++) {
                            std::cerr << indentation << "    Element " << j << ":" << std::endl;
                            const google::protobuf::Message& nested_message =
                                reflection->GetRepeatedMessage(message, field, j);
                            PrintMessageFields(nested_message, indent + 6);
                        }
                    } else {
                        std::cerr << indentation << "    Structure of repeated message element:" << std::endl;
                        const google::protobuf::Message& prototype =
                            *reflection->GetMessageFactory()->GetPrototype(field->message_type());
                        PrintMessageFields(prototype, indent + 6);
                    }
                } else {
                    std::cerr << std::endl;
                }
            } else {
                if (reflection->HasField(message, field)) {
                    std::cerr << " (has value)";

                    if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                        std::cerr << std::endl;
                        const google::protobuf::Message& nested_message =
                            reflection->GetMessage(message, field);
                        PrintMessageFields(nested_message, indent + 4);
                    } else {
                        std::cerr << std::endl;
                    }
                } else {
                    std::cerr << " (not set)";

                    if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                        std::cerr << std::endl;
                        std::cerr << indentation << "    Structure of nested message:" << std::endl;
                        PrintMessageFields(*reflection->GetMessageFactory()->GetPrototype(field->message_type()), indent + 6);
                    } else {
                        std::cerr << std::endl;
                    }
                }
            }

            if (!field->is_repeated() && field->has_default_value() &&
                field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                std::cerr << indentation << "    Default value: ";
                switch (field->type()) {
                    case google::protobuf::FieldDescriptor::TYPE_INT32:
                    case google::protobuf::FieldDescriptor::TYPE_INT64:
                    case google::protobuf::FieldDescriptor::TYPE_UINT32:
                    case google::protobuf::FieldDescriptor::TYPE_UINT64:
                        std::cerr << field->default_value_int64();
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                        std::cerr << field->default_value_double();
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_BOOL:
                        std::cerr << (field->default_value_bool() ? "true" : "false");
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_STRING:
                    case google::protobuf::FieldDescriptor::TYPE_BYTES:
                        std::cerr << "\"" << field->default_value_string() << "\"";
                        break;
                    default:
                        std::cerr << "[complex type]";
                }
                std::cerr << std::endl;
            }
        }

        for (int i = 0; i < descriptor->enum_type_count(); i++) {
            const google::protobuf::EnumDescriptor* enum_type = descriptor->enum_type(i);
            std::cerr << indentation << "  Enum: " << enum_type->name() << std::endl;
            for (int j = 0; j < enum_type->value_count(); j++) {
                std::cerr << indentation << "    " << enum_type->value(j)->name()
                        << " = " << enum_type->value(j)->number() << std::endl;
            }
        }

        for (int i = 0; i < descriptor->nested_type_count(); i++) {
            const google::protobuf::Descriptor* nested_type = descriptor->nested_type(i);
            std::cerr << indentation << "  Nested message type: " << nested_type->name() << std::endl;
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillAuthentication() {
        static constexpr auto extractSubjectType = [](const TString& authType) {
            using ESubjectType = ::yandex::cloud::events::Authentication;
            static const TMap<TString, ESubjectType::SubjectType> Types {
                {"service_account", ESubjectType::SERVICE_ACCOUNT},
                {"federated_account", ESubjectType::FEDERATED_USER_ACCOUNT},
                {"user_account", ESubjectType::YANDEX_PASSPORT_USER_ACCOUNT},
            };
            return Types.Value(authType, ESubjectType::SUBJECT_TYPE_UNSPECIFIED);
        };
        Ev.mutable_authentication()->set_authenticated(true);
        Ev.mutable_authentication()->set_subject_id(EventInfo.UserSID);
        Ev.mutable_authentication()->set_subject_type(extractSubjectType(EventInfo.AuthType));
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillAuthorization() {
        // authorized = true;
        // permissions.permission: см. permissions из SearchEvents;
        // permissions.resource_type = resource_type из SearchEvents;
        // permissions.resource_id = resource_id из SearchEvents;
        // permissions.authorized оставляем пустым.

        Ev.mutable_authorization()->set_authorized(true);
        auto* permission = Ev.mutable_authorization()->add_permissions();
            permission->set_permission(EventInfo.Permission);
            permission->set_resource_type(EventInfo.ResourceType);
            permission->set_resource_id(EventInfo.FolderId);

        if (!EventInfo.UserSanitizedToken.empty()) {
            Ev.mutable_authorization()->mutable_token_info()->set_masked_iam_token(EventInfo.UserSanitizedToken);
        }
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
            Ev.mutable_error()->set_message(EventInfo.Issue);
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
        PrintMessageFields(ev);
    }

    template void TAuditSender::Send<TCreateQueueEvent>(const TCreateQueueEvent&);
    template void TAuditSender::Send<TUpdateQueueEvent>(const TUpdateQueueEvent&);
    template void TAuditSender::Send<TDeleteQueueEvent>(const TDeleteQueueEvent&);

// ===============================================================
    TString TProcessor::GetFullTablePath() const {
        if (!Root.empty()) {
            return TStringBuilder() << Root << "/" << EventTableName;
        } else {
            return TString(EventTableName);
        }
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
        const TString& root,
        const TString& database,
        const TDuration& retryTimeout
    )
        : Root(root)
        , Database(database)
        , RetryTimeout(retryTimeout)
        , SelectQuery(GetInitSelectQuery())
        , DeleteQuery(GetInitDeleteQuery())
    {
    }

    void TProcessor::RunQuery(TString query, std::unique_ptr<NYdb::TParams> params, bool readOnly) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();

        request->SetKeepSession(true); // todo : fix

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
        Schedule(RetryTimeout, new TEvents::TEvWakeup);
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
        Schedule(RetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }

    void TProcessor::HandleUndelivered(const NActors::TEvents::TEvUndelivered::TPtr&) {
        ProcessFailure();
    }

    void TProcessor::HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&) {
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
            cloudEvent.QueueName = *parser.ColumnParser(1).GetOptionalUtf8();
            cloudEvent.CreatedAt = *parser.ColumnParser(2).GetOptionalUint64();
            TString type = *parser.ColumnParser(3).GetOptionalUtf8();
            cloudEvent.Type = DefaultEventTypePrefix + type;

            cloudEvent.Id = convertId(cloudEvent.OriginalId, cloudEvent.Type, cloudEvent.CreatedAt);

            cloudEvent.CloudId = *parser.ColumnParser(4).GetOptionalUtf8();
            cloudEvent.FolderId = *parser.ColumnParser(5).GetOptionalUtf8();
            cloudEvent.UserSID = *parser.ColumnParser(6).GetOptionalUtf8();
            cloudEvent.UserSanitizedToken = *parser.ColumnParser(7).GetOptionalUtf8();
            cloudEvent.AuthType = *parser.ColumnParser(8).GetOptionalUtf8();
            cloudEvent.RemoteAddress = *parser.ColumnParser(9).GetOptionalUtf8();
            cloudEvent.RequestId = *parser.ColumnParser(10).GetOptionalUtf8();
            cloudEvent.IdempotencyId = *parser.ColumnParser(11).GetOptionalUtf8();
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

            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::SQS, error);

            ProcessFailure();
            return;
        }

        const auto& response = record.GetResponse();

        auto eventsList = ConvertSelectResponseToEventList(response);
        UpdateSessionId(ev);

        if (!eventsList.empty()) {
            for (const auto& cloudEvent : eventsList) {
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
                    Y_UNREACHABLE();
                }
            }

            NYdb::TParamsBuilder paramsBuilder;

            auto& param = paramsBuilder.AddParam("$Events");
            param.BeginList();

            for (const auto& cloudEv : eventsList) {
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
        }

        Become(&TProcessor::StateWaitDeleteResponse);
    }

    void TProcessor::HandleDeleteResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            TString error = "DeleteResponse: Failed.\n";
            for (const auto& issue : record.GetResponse().GetQueryIssues()) {
                error += issue.message() + "\n";
            }

            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::SQS, error);

            ProcessFailure();
            return;
        }

        UpdateSessionId(ev);

        Schedule(RetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }
} // namespace NCloudEvents
} // namespace NKikimr::NSQS
