#include "cloud_events.h"

#include <ydb/core/audit/audit_log.h>
#include <google/protobuf/util/time_util.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
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

        if (!EventInfo.MaskedToken.empty()) {
            Ev.mutable_authentication()->mutable_token_info()->set_masked_iam_token(EventInfo.MaskedToken);
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillAuthorization() {
        Ev.mutable_authorization()->set_authorized(true);

        auto* permission = Ev.mutable_authorization()->add_permissions();
        permission->set_permission(EventInfo.Permission);
        permission->set_resource_type(EventInfo.ResourceType);
        permission->set_resource_id(EventInfo.ResourceId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillEventMetadata() {
        Ev.mutable_event_metadata()->set_event_id(EventInfo.Id);
        Ev.mutable_event_metadata()->set_event_type(TString("yandex.cloud.events.ymq.") + EventInfo.Type);
        const auto createdAt_proto = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(EventInfo.CreatedAt.MilliSeconds());
        *Ev.mutable_event_metadata()->mutable_created_at() = createdAt_proto;
        Ev.mutable_event_metadata()->set_cloud_id(EventInfo.CloudId);
        Ev.mutable_event_metadata()->set_folder_id(EventInfo.FolderId);
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillRequestMetadata() {
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
            Ev.mutable_error()->set_message(EventInfo.Issue);
        }
    }

    template<typename TProtoEvent>
    void TFiller<TProtoEvent>::FillDetails() {
        Ev.mutable_details()->set_name(EventInfo.QueueName);
        /*
            Historically, the queue’s “internal” name has also served as its identifier.

            In a Yandex.Cloud installation, this queue name is actually the queue’s resource_id.

            If you look closely at the current file, you can see that the ResourceId value is passed
            in the Authorization section. However, it’s important to understand that, logically,
            the protobuf field resource_id refers to identifying the resource itself and
            is used for permission checks. The queue_id field, on the other hand, logically means
            this is an identifier not of the resource in general, but specifically of the queue.
            So even though these two values are the same, they represent completely different things conceptually.

            In addition, there is also a real need to single out the queue’s own identifier.
            This arose due to the specifics of how cloud events are processed by cloud subsystems.
        */
        Ev.mutable_details()->set_queue_id(EventInfo.ResourceId);

        auto convertLabels = [](const TString& str) -> THashMap<TBasicString<char>, NJson::TJsonValue> {
            NJson::TJsonValue json;
            NJson::ReadJsonTree(str, &json);
            return json.GetMap();
        };

        THashMap<TBasicString<char>, NJson::TJsonValue> protoLabels = convertLabels(EventInfo.Labels);

        for (const auto& [k, jsonValue] : protoLabels) {
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

    template class TFiller<TCreateQueueEvent>;
    template class TFiller<TUpdateQueueEvent>;
    template class TFiller<TDeleteQueueEvent>;

    template<typename TProtoEvent>
    void TAuditSender::SendProto(const TProtoEvent& ev, IEventsWriterWrapper::TPtr writer) {
        if (writer) {
            TString jsonEv;
            google::protobuf::util::JsonPrintOptions printOpts;
            printOpts.preserve_proto_field_names = true;
            printOpts.always_print_primitive_fields = true;
            auto status = google::protobuf::util::MessageToJsonString(ev, &jsonEv, printOpts);
            Y_ASSERT(status.ok());
            writer->Write(jsonEv);
        }
    }

    template void TAuditSender::SendProto<TCreateQueueEvent>(const TCreateQueueEvent&, IEventsWriterWrapper::TPtr);
    template void TAuditSender::SendProto<TUpdateQueueEvent>(const TUpdateQueueEvent&, IEventsWriterWrapper::TPtr);
    template void TAuditSender::SendProto<TDeleteQueueEvent>(const TDeleteQueueEvent&, IEventsWriterWrapper::TPtr);

// ===============================================================

    /*
        This function returns only one random number
        intended to identify an event of the CloudEvent type.

        However, it is worth noting that the factor that outputs logs
        to the unified agent actually generates a more complex form of the event id.

        So, GenerateEventCloudId does not GenerateEventCloudId the 'real' CloudEvent id.
    */
    ui64 TEventIdGenerator::Generate() {
        return RandomNumber<ui64>();
    }

    void TAuditSender::Send(const TEventInfo& evInfo, IEventsWriterWrapper::TPtr writer) {
        static constexpr TStringBuf componentName = "ymq";
        static const     TString emptyValue = "{none}";

        AUDIT_LOG(
            AUDIT_PART("component", componentName)
            AUDIT_PART("id", evInfo.Id)
            AUDIT_PART("operation", evInfo.Type)
            AUDIT_PART("status", evInfo.Issue.empty() ? "SUCCESS" : "ERROR")
            AUDIT_PART("reason", evInfo.Issue, !evInfo.Issue.empty())
            AUDIT_PART("remote_address", evInfo.RemoteAddress)
            AUDIT_PART("subject", evInfo.UserSID)
            AUDIT_PART("masked_token", (!evInfo.MaskedToken.empty() ? evInfo.MaskedToken : emptyValue))
            AUDIT_PART("auth_type", (!evInfo.AuthType.empty() ? evInfo.AuthType : emptyValue))
            AUDIT_PART("permission", evInfo.Permission)
            AUDIT_PART("created_at", evInfo.CreatedAt.ToString())
            AUDIT_PART("cloud_id", (!evInfo.CloudId.empty() ? evInfo.CloudId : emptyValue))
            AUDIT_PART("folder_id", (!evInfo.FolderId.empty() ? evInfo.FolderId : emptyValue))
            AUDIT_PART("resource_id", (!evInfo.ResourceId.empty() ? evInfo.ResourceId : emptyValue))
            AUDIT_PART("request_id", evInfo.RequestId)
            AUDIT_PART("idempotency_id", (!evInfo.IdempotencyId.empty() ? evInfo.IdempotencyId : emptyValue))
            AUDIT_PART("queue", (!evInfo.QueueName.empty() ? evInfo.QueueName : emptyValue))
            AUDIT_PART("labels", evInfo.Labels)
        );

        if (evInfo.Type == "CreateMessageQueue") {
            TCreateQueueEvent ev;
            TFiller<TCreateQueueEvent> filler(evInfo, ev);
            filler.Fill();
            TAuditSender::SendProto<TCreateQueueEvent>(ev, writer);
        } else if (evInfo.Type == "UpdateMessageQueue") {
            TUpdateQueueEvent ev;
            TFiller<TUpdateQueueEvent> filler(evInfo, ev);
            filler.Fill();
            TAuditSender::SendProto<TUpdateQueueEvent>(ev, writer);
        } else if (evInfo.Type == "DeleteMessageQueue") {
            TDeleteQueueEvent ev;
            TFiller<TDeleteQueueEvent> filler(evInfo, ev);
            filler.Fill();
            TAuditSender::SendProto<TDeleteQueueEvent>(ev, writer);
        } else {
            Y_UNREACHABLE();
        }
    }

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
            << "CreatedAt,"
            << "Id,"
            << "QueueName,"
            << "Type,"
            << "CloudId,"
            << "FolderId,"
            << "ResourceId,"
            << "UserSID,"
            << "MaskedToken,"
            << "AuthType,"
            << "PeerName,"
            << "RequestId,"
            << "Labels" << "\n"
            << "FROM `" << GetFullTablePath() << "`;\n";
    }

    TString TProcessor::GetInitDeleteQuery() const {
        return TStringBuilder()
            << "--!syntax_v1" << "\n"
            << "DECLARE $Events AS List<Struct<CreatedAt:Uint64, Id:Uint64>>;" << "\n"
            << "$EventsSource = (SELECT item.CreatedAt AS CreatedAt, item.Id AS Id" << "\n"
                             << "FROM (SELECT $Events AS events) FLATTEN LIST BY events as item);" << "\n"
            << "DELETE FROM `" << GetFullTablePath() << "`" << "\n"
            << "ON SELECT * FROM $EventsSource;" << "\n";
    }

    TProcessor::TProcessor
    (
        const TString& root,
        const TString& database,
        const TDuration& retryTimeout,
        IEventsWriterWrapper::TPtr eventsWriter
    )
        : Root(root)
        , Database(database)
        , RetryTimeout(retryTimeout)
        , SelectQuery(GetInitSelectQuery())
        , DeleteQuery(GetInitDeleteQuery())
        , EventsWriter(eventsWriter)
    {
    }

    TProcessor::~TProcessor()
    {
        if (EventsWriter) {
            EventsWriter->Close();
        }
    }

    void TProcessor::RunQuery(TString query, std::unique_ptr<NYdb::TParams> params) {
        bool isDeleteQuery = static_cast<bool>(params);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();

        request->SetKeepSession(!isDeleteQuery);

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

        if (isDeleteQuery) {
            request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        } else {
            request->MutableTxControl()->mutable_begin_tx()->mutable_online_read_only();
        }

        request->MutableTxControl()->set_commit_tx(true);

        if (isDeleteQuery) {
            request->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(*params)));
        }

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release(), IEventHandle::FlagTrackDelivery);
    }

    void TProcessor::MakeDeleteResponse() {
        NYdb::TParamsBuilder paramsBuilder;

        auto& param = paramsBuilder.AddParam("$Events");
        param.BeginList();

        for (const auto& cloudEv : Events) {
            param.AddListItem()
                .BeginStruct()
                .AddMember("CreatedAt")
                    .Uint64(cloudEv.CreatedAt.MilliSeconds())
                .AddMember("Id")
                    .Uint64(cloudEv.OriginalId)
                .EndStruct();
        }

        param.EndList();
        param.Build();

        auto params = paramsBuilder.Build();
        RunQuery(DeleteQuery, std::make_unique<decltype(params)>(params));
        Become(&TProcessor::StateWaitDeleteResponse);
    }

    void TProcessor::StopSession() {
        if (!SessionId.empty()) {
            auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
            SessionId = TString();
        }
    }

    void TProcessor::PutToSleep() {
        Events.clear();
        StopSession();
        Schedule(RetryTimeout, new TEvents::TEvWakeup);
        Become(&TProcessor::StateWaitWakeUp);
    }

    void TProcessor::Bootstrap() {
        PutToSleep();
    }

    void TProcessor::HandleUndelivered(const NActors::TEvents::TEvUndelivered::TPtr&) {
        PutToSleep();
    }

    void TProcessor::HandleWakeup(const NActors::TEvents::TEvWakeup::TPtr&) {
        RunQuery(SelectQuery);
        Become(&TProcessor::StateWaitSelectResponse);
    }

    std::vector<TEventInfo> TProcessor::ConvertSelectResponseToEventList(const ::NKikimrKqp::TQueryResponse& response) {
        std::vector<TEventInfo> result;

        Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
        NYdb::TResultSetParser parser(response.GetYdbResults(0));

        auto convertId = [](ui64 id, const TString& type, TInstant CreatedAt) -> TString {
            return TStringBuilder() << id << "$" << type << "$" << CreatedAt.ToString();
        };

        while (parser.TryNextRow()) {
            auto& cloudEvent = result.emplace_back();

            cloudEvent.CreatedAt = TInstant::MilliSeconds(*parser.ColumnParser(0).GetOptionalUint64());
            cloudEvent.OriginalId = *parser.ColumnParser(1).GetOptionalUint64();
            cloudEvent.QueueName = *parser.ColumnParser(2).GetOptionalUtf8();
            cloudEvent.Type = *parser.ColumnParser(3).GetOptionalUtf8();

            if (cloudEvent.Type == "CreateMessageQueue") {
                cloudEvent.Permission = "ymq.queues.create";
            } else if (cloudEvent.Type == "UpdateMessageQueue") {
                cloudEvent.Permission = "ymq.queues.setAttributes";
            } else if (cloudEvent.Type == "DeleteMessageQueue") {
                cloudEvent.Permission = "ymq.queues.delete";
            }

            cloudEvent.IdempotencyId = convertId(cloudEvent.OriginalId, cloudEvent.Type, cloudEvent.CreatedAt);
            cloudEvent.Id = convertId(cloudEvent.OriginalId, cloudEvent.Type, TInstant::Now());

            cloudEvent.CloudId = *parser.ColumnParser(4).GetOptionalUtf8();
            cloudEvent.FolderId = *parser.ColumnParser(5).GetOptionalUtf8();
            cloudEvent.ResourceId = *parser.ColumnParser(6).GetOptionalUtf8();
            cloudEvent.UserSID = *parser.ColumnParser(7).GetOptionalUtf8();
            cloudEvent.MaskedToken = *parser.ColumnParser(8).GetOptionalUtf8();
            cloudEvent.AuthType = *parser.ColumnParser(9).GetOptionalUtf8();
            cloudEvent.RemoteAddress = *parser.ColumnParser(10).GetOptionalUtf8();
            cloudEvent.RequestId = *parser.ColumnParser(11).GetOptionalUtf8();
            cloudEvent.Labels = *parser.ColumnParser(12).GetOptionalUtf8();
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
            TString error = "SelectResponse: Failed.\n";
            for (const auto& issue : record.GetResponse().GetQueryIssues()) {
                error += issue.message() + "\n";
            }

            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::SQS, error);

            PutToSleep();
            return;
        }

        const auto& response = record.GetResponse();

        Events = ConvertSelectResponseToEventList(response);

        UpdateSessionId(ev);

        if (!Events.empty()) {
            for (const auto& cloudEvent : Events) {
                TAuditSender::Send(cloudEvent, EventsWriter);
            }

            MakeDeleteResponse();
        } else {
            PutToSleep();
        }
    }

    void TProcessor::HandleDeleteResponse(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (const auto& record = ev->Get()->Record; record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            TString error = "DeleteResponse: Failed.\n";
            for (const auto& issue : record.GetResponse().GetQueryIssues()) {
                error += issue.message() + "\n";
            }

            LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::SQS, error);

            StopSession();
            MakeDeleteResponse();
        } else {
            PutToSleep();
        }
    }
} // namespace NCloudEvents
} // namespace NKikimr::NSQS
