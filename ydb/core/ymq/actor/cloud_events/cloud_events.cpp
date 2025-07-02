#include "cloud_events.h"

#include <ydb/core/audit/audit_log.h>

namespace NKikimr::NSQS {
namespace NCloudEvents {
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

    void TAuditSender::Send(const TEventInfo& evInfo) {
        static constexpr TStringBuf componentName = "ymq";
        static const     TString emptyValue = "{none}";

        AUDIT_LOG(
            AUDIT_PART("component", componentName)
            AUDIT_PART("id", evInfo.Id)
            AUDIT_PART("operation", evInfo.Type)
            AUDIT_PART("status", TString(evInfo.Issue.empty() ? "SUCCESS" : "ERROR"))
            AUDIT_PART("reason", evInfo.Issue, !evInfo.Issue.empty())
            AUDIT_PART("remote_address", evInfo.RemoteAddress)
            AUDIT_PART("subject", evInfo.UserSID)
            AUDIT_PART("masked_token", (!evInfo.MaskedToken.empty() ? evInfo.MaskedToken : emptyValue))
            AUDIT_PART("auth_type", (!evInfo.AuthType.empty() ? evInfo.AuthType : emptyValue))
            AUDIT_PART("permission", evInfo.Permission)
            AUDIT_PART("created_at", ::ToString(evInfo.CreatedAt))
            AUDIT_PART("cloud_id", (!evInfo.CloudId.empty() ? evInfo.CloudId : emptyValue))
            AUDIT_PART("folder_id", (!evInfo.FolderId.empty() ? evInfo.FolderId : emptyValue))
            AUDIT_PART("resource_id", (!evInfo.ResourceId.empty() ? evInfo.ResourceId : emptyValue))
            AUDIT_PART("request_id", evInfo.RequestId)
            AUDIT_PART("idempotency_id", (!evInfo.IdempotencyId.empty() ? evInfo.IdempotencyId : emptyValue))
            AUDIT_PART("queue", (!evInfo.QueueName.empty() ? evInfo.QueueName : emptyValue))
            AUDIT_PART("labels", evInfo.Labels)
        );
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
        const TDuration& retryTimeout
    )
        : Root(root)
        , Database(database)
        , RetryTimeout(retryTimeout)
        , SelectQuery(GetInitSelectQuery())
        , DeleteQuery(GetInitDeleteQuery())
    {
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
                    .Uint64(cloudEv.CreatedAt)
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

        auto convertId = [](ui64 id, const TString& type, ui64 createdAt) -> TString {
            return TStringBuilder() << id << "$" << type << "$" << createdAt;
        };

        while (parser.TryNextRow()) {
            auto& cloudEvent = result.emplace_back();

            cloudEvent.CreatedAt = *parser.ColumnParser(0).GetOptionalUint64();
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
            cloudEvent.Id = convertId(cloudEvent.OriginalId, cloudEvent.Type, TInstant::Now().MilliSeconds());

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
                TAuditSender::Send(cloudEvent);
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
