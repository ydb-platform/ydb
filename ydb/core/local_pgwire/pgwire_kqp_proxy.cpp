#include "log_impl.h"
#include "local_pgwire_util.h"
#include "pgwire_kqp_proxy.h"
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/pgproxy/pg_proxy_types.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

template<typename Base, typename TRequestEventType>
class TPgwireKqpProxy : public TActorBootstrapped<Base> {
protected:
    using TBase = TActorBootstrapped<Base>;
    using TRequestEventPtr = typename TRequestEventType::TPtr;
    using TResponseEventPtr = decltype(std::declval<TRequestEventType>().Reply());

    TActorId Owner_;
    std::unordered_map<TString, TString> ConnectionParams_;
    TConnectionState Connection_;
    NKikimrKqp::EQueryAction QueryAction_ = {};
    TRequestEventPtr EventRequest_;
    bool NeedMeta_ = false;
    std::size_t RowsSelected_ = 0;
    TResponseEventPtr Response_;
    std::vector<int16_t> ResponseFormat_;

    TPgwireKqpProxy(const TActorId owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, TRequestEventPtr&& ev)
        : Owner_(owner)
        , ConnectionParams_(std::move(params))
        , Connection_(connection)
        , EventRequest_(std::move(ev))
        , Response_(EventRequest_->Get()->Reply())
    {
        if (!Connection_.Transaction.Status) {
            Connection_.Transaction.Status = 'I';
        }
    }

    TString ToUpperASCII(TStringBuf s) {
        TString r;
        r.resize(s.size());
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] <= 0x7f && s[i] >= 0x20) {
                r[i] = toupper(s[i]);
            } else {
                r[i] = s[i];
            }
        }
        return r;
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> MakeKqpRequest() {
        TString database;
        if (ConnectionParams_.count("database")) {
            database = ConnectionParams_["database"];
        }
        TString token;
        if (ConnectionParams_.count("ydb-serialized-token")) {
            token = ConnectionParams_["ydb-serialized-token"];
        }
        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetDatabase(database);
        if (Connection_.SessionId) {
            request.SetSessionId(Connection_.SessionId);
        }
        request.SetKeepSession(true);
        event->Record.SetUserToken(token);
        return event;
    }

    TResponseEventPtr MakeResponse() {
        return EventRequest_->Get()->Reply();
    }

    THolder<NKqp::TEvKqp::TEvQueryRequest> ConvertQueryToRequest(TStringBuf query) {
        // HACK
        TString q(ToUpperASCII(query.substr(0, 20)));
        if (q.StartsWith("BEGIN")) {
            Response_->Tag = "BEGIN";
            if (Connection_.Transaction.Status == 'I') {
                auto event = MakeKqpRequest();
                NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
                request.SetAction(QueryAction_ = NKikimrKqp::QUERY_ACTION_BEGIN_TX);
                request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
                return event;
            } else if (Connection_.Transaction.Status == 'E') {
                Response_->ErrorFields.push_back({'E', "ERROR"});
                Response_->ErrorFields.push_back({'M', "Current transaction is aborted, commands ignored until end of transaction block"});
                return {};
            } else if (Connection_.Transaction.Status == 'T') {
                Response_->NoticeFields.push_back({'S', "WARNING"});
                Response_->NoticeFields.push_back({'M', "There is already a transaction in progress"});
                return {};
            }
        } else if (q.StartsWith("COMMIT") || q.StartsWith("END")) {
            Response_->Tag = "COMMIT";
            if (Connection_.Transaction.Status == 'T') {
                // in transaction
                auto event = MakeKqpRequest();
                NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
                request.SetAction(QueryAction_ = NKikimrKqp::QUERY_ACTION_COMMIT_TX);
                request.MutableTxControl()->set_tx_id(Connection_.Transaction.Id);
                request.MutableTxControl()->set_commit_tx(true);
                return event;
            } else if (Connection_.Transaction.Status == 'E') {
                // in error transaction
                // ignore, reset to I
                Connection_.Transaction.Status = 'I';
                return {};
            } else if (Connection_.Transaction.Status == 'I') {
                Response_->NoticeFields.push_back({'S', "WARNING"});
                Response_->NoticeFields.push_back({'M', "There is no transaction in progress"});
                return {};
            }
        } else if (q.StartsWith("ROLLBACK")) {
            Response_->Tag = "ROLLBACK";
            if (Connection_.Transaction.Status == 'T') {
                // in transaction
                auto event = MakeKqpRequest();
                NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
                request.SetAction(QueryAction_ = NKikimrKqp::QUERY_ACTION_ROLLBACK_TX);
                request.MutableTxControl()->set_tx_id(Connection_.Transaction.Id);
                return event;
            } else if (Connection_.Transaction.Status == 'E') {
                // ignore, reset to I
                Connection_.Transaction.Status = 'I';
                return {};
            } else if (Connection_.Transaction.Status == 'I') {
                Response_->NoticeFields.push_back({'S', "WARNING"});
                Response_->NoticeFields.push_back({'M', "There is no transaction in progress"});
                return {};
            }
        } else {
            if (Connection_.Transaction.Status == 'E') {
                Response_->ErrorFields.push_back({'E', "ERROR"});
                Response_->ErrorFields.push_back({'M', "Current transaction is aborted, commands ignored until end of transaction block"});
                return {};
            }
            auto event = MakeKqpRequest();
            NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
            request.SetAction(QueryAction_ = NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            if (!q.StartsWith("CREATE") && !q.StartsWith("ALTER") && !q.StartsWith("DROP")) {
                request.SetUsePublicResponseDataFormat(true);
                request.MutableQueryCachePolicy()->set_keep_in_cache(true);
                if (Connection_.Transaction.Status == 'I') {
                    request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
                    request.MutableTxControl()->set_commit_tx(true);
                } else if (Connection_.Transaction.Status == 'T') {
                    request.MutableTxControl()->set_tx_id(Connection_.Transaction.Id);
                }
            }
            // TODO(xenoxeno): check ConnectionParams_ to support different syntax
            request.SetSyntax(Ydb::Query::SYNTAX_PG);
            request.SetQuery(TString(query));
            return event;
        }
        return {};
    }

    void SendToKQP(THolder<NKqp::TEvKqp::TEvQueryRequest>&& event) {
        ActorIdToProto(TBase::SelfId(), event->Record.MutableRequestActorId());
        BLOG_D("Sent event to kqpProxy " << event->Record.ShortDebugString());
        TBase::Send(NKqp::MakeKqpProxyID(TBase::SelfId().NodeId()), event.Release());
    }

    void OnErrorTransaction() {
        if (Response_->Tag == "COMMIT") {
            Response_->Tag = "ROLLBACK";
        }
    }

    void UpdateConnectionWithKqpResponse(const NKikimrKqp::TEvQueryResponse& record) {
        Connection_.SessionId = record.GetResponse().GetSessionId();

        if (Connection_.Transaction.Status != 'E') {
            if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                Connection_.Transaction.Id = record.GetResponse().GetTxMeta().id();
                if (Connection_.Transaction.Id && QueryAction_ != NKikimrKqp::QUERY_ACTION_COMMIT_TX && QueryAction_ != NKikimrKqp::QUERY_ACTION_ROLLBACK_TX) {
                    Connection_.Transaction.Status = 'T';
                } else {
                    Connection_.Transaction.Status = 'I';
                }
            } else {
                if (Connection_.Transaction.Id) {
                    Connection_.Transaction.Id.clear();
                    Connection_.Transaction.Status = 'E';
                } else {
                    Connection_.Transaction.Status = 'I';
                }
            }
        } else {
            OnErrorTransaction();
        }
    }

    static void FillError(const NKikimrKqp::TEvQueryResponse& record, typename TResponseEventPtr::element_type& response) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        NYdb::TStatus status(NYdb::EStatus(record.GetYdbStatus()), std::move(issues));
        TString message(TStringBuilder() << status);
        response.ErrorFields.push_back({'E', "ERROR"});
        response.ErrorFields.push_back({'M', message});
        if (message.find("Error: Cannot find table") != TString::npos) {
            response.ErrorFields.push_back({'C', "42P01"});
        }
        if (record.GetYdbStatus() == Ydb::StatusIds::INTERNAL_ERROR) {
            response.ErrorFields.push_back({'C', "XX000"});
            response.DropConnection = true;
        }
        if (record.GetYdbStatus() == Ydb::StatusIds::BAD_SESSION) {
            response.ErrorFields.push_back({'C', "08006"});
            response.DropConnection = true;
        }
    }

    static void FillMeta(const NYdb::TResultSet& resultSet, NPG::TEvPGEvents::TEvQueryResponse* response) {
        for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
            std::optional<NYdb::TPgType> pgType = GetPgTypeFromYdbType(column.Type);
            if (pgType.has_value()) {
                response->DataFields.push_back({
                    .Name = column.Name,
                    .DataType = pgType->Oid,
                    .DataTypeSize = pgType->Typlen,
                    .DataTypeModifier = pgType->Typmod,
                });
            } else {
                response->DataFields.push_back({
                    .Name = column.Name
                });
            }
        }
    }

    static void FillMeta(const NYdb::TResultSet&, NPG::TEvPGEvents::TEvExecuteResponse*) {
        // no meta for prepared execute
    }

    void ReplyWithResponseAndPassAway() {
        Response_->TransactionStatus = Connection_.Transaction.Status;
        TBase::Send(Owner_, new TEvEvents::TEvProxyCompleted(Connection_));
        BLOG_D("Finally replying to " << EventRequest_->Sender << " cookie " << EventRequest_->Cookie);
        TBase::Send(EventRequest_->Sender, Response_.release(), 0, EventRequest_->Cookie);
        TBase::PassAway();
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        NYdb::TResultSet resultSet(std::move(*ev->Get()->Record.MutableResultSet()));
        auto response = MakeResponse();
        if (NeedMeta_) {
            FillMeta(resultSet, response.get());
            NeedMeta_ = false;
        }
        FillResultSet(resultSet, response.get()->DataRows, ResponseFormat_);
        response->CommandCompleted = false;
        response->ReadyForQuery = false;

        RowsSelected_ += response->DataRows.size();

        BLOG_D(this->SelfId() << " Send rowset " << ev->Get()->Record.GetQueryResultIndex() << " data " << ev->Get()->Record.GetSeqNo() << " to " << EventRequest_->Sender);
        TBase::Send(EventRequest_->Sender, response.release(), 0, EventRequest_->Cookie);

        BLOG_D(this->SelfId() << " Send stream data ack to " << ev->Sender);
        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(std::numeric_limits<i64>::max());
        TBase::Send(ev->Sender, resp.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse " << ev->Get()->Record.ShortDebugString());
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        if (record.GetResponse().HasExtraInfo()) {
            const auto& extraInfo = record.GetResponse().GetExtraInfo();
            if (extraInfo.HasPgInfo() && extraInfo.GetPgInfo().HasCommandTag()) {
                Response_->Tag = extraInfo.GetPgInfo().GetCommandTag();
            }
        }
        UpdateConnectionWithKqpResponse(record);
        try {
            if (record.HasYdbStatus()) {
                if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    BLOG_ENSURE(record.GetResponse().GetYdbResults().empty());

                    // HACK
                    if (Response_->Tag == "SELECT") {
                        Response_->Tag = TStringBuilder() << Response_->Tag << " " << RowsSelected_;
                    }
                } else {
                    FillError(record, *Response_);
                }
            } else {
                Response_->ErrorFields.push_back({'E', "ERROR"});
                Response_->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            Response_->ErrorFields.push_back({'E', "ERROR"});
            Response_->ErrorFields.push_back({'M', e.what()});
        }
        Response_->CommandCompleted = Response_->ErrorFields.empty();
        return ReplyWithResponseAndPassAway();
    }

    void Handle(TEvEvents::TEvCancelRequest::TPtr&) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCancelQueryRequest>();
        if (Connection_.SessionId) {
            ev->Record.MutableRequest()->SetSessionId(Connection_.SessionId);
        }
        BLOG_D("Sent CancelQueryRequest to kqpProxy " << ev->Record.ShortDebugString());
        TBase::Send(NKqp::MakeKqpProxyID(TBase::SelfId().NodeId()), ev.Release());

        Response_->ErrorFields.push_back({'S', "ERROR"});
        Response_->ErrorFields.push_back({'V', "ERROR"});
        Response_->ErrorFields.push_back({'C', "57014"});
        Response_->ErrorFields.push_back({'M', "Cancelling statement due to user request"});
        return ReplyWithResponseAndPassAway();
    }
};

class TPgwireKqpProxyQuery : public TPgwireKqpProxy<TPgwireKqpProxyQuery, TEvEvents::TEvSingleQuery> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyQuery, TEvEvents::TEvSingleQuery>;

public:
    TPgwireKqpProxyQuery(const TActorId& owner,
                         std::unordered_map<TString, TString> params,
                         const TConnectionState& connection,
                         TEvEvents::TEvSingleQuery::TPtr&& evQuery)
        : TPgwireKqpProxy(owner, std::move(params), connection, std::move(evQuery))
    {
        NeedMeta_ = true;
        Response_->ReadyForQuery = EventRequest_->Get()->FinalQuery;
    }

    void Bootstrap() {
        auto query(EventRequest_->Get()->Query);
        auto event = ConvertQueryToRequest(query);
        if (event) {
            SendToKQP(std::move(event));
        } else {
            return ReplyWithResponseAndPassAway();
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyQuery::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, TBase::Handle);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, TBase::Handle);
            hFunc(TEvEvents::TEvCancelRequest, Handle);
        }
    }
};

class TPgwireKqpProxyParse : public TPgwireKqpProxy<TPgwireKqpProxyParse, NPG::TEvPGEvents::TEvParse> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyParse, NPG::TEvPGEvents::TEvParse>;

    NPG::TPGParse::TQueryData QueryData_;

public:
    TPgwireKqpProxyParse(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvParse::TPtr&& evParse)
        : TPgwireKqpProxy(owner, std::move(params), connection, std::move(evParse))
        , QueryData_(EventRequest_->Get()->Message->GetQueryData())
    {
    }

    void Bootstrap() {
        auto event = ConvertQueryToRequest(QueryData_.Query);
        if (event) {
            NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
            if (request.GetType() == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY) {
                request.SetAction(QueryAction_ = NKikimrKqp::QUERY_ACTION_EXPLAIN);
                SendToKQP(std::move(event));
            } else { // for DDL and TCL
                BLOG_D("Skipping parse of DDL/TCL");
                TParsedStatement statement;
                statement.QueryData = std::move(QueryData_);
                Send(Owner_, new TEvEvents::TEvUpdateStatement(statement));
                return ReplyWithResponseAndPassAway();
            }
        } else {
            return ReplyWithResponseAndPassAway();
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyParse::StateWork);
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse " << ev->Get()->Record.ShortDebugString());
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        try {
            if (record.HasYdbStatus()) {
                if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    TParsedStatement statement;
                    statement.QueryData = std::move(QueryData_);
                    for (const auto& param : record.GetResponse().GetQueryParameters()) {
                        Ydb::Type ydbType;
                        ConvertMiniKQLTypeToYdbType(param.GetType(), ydbType);
                        statement.ParameterTypes.push_back(ydbType);
                    }
                    for (const auto& result : record.GetResponse().GetYdbResults()) {
                        for (const auto& column : result.columns()) {
                            std::optional<NYdb::TPgType> pgType = GetPgTypeFromYdbType(column.type());
                            if (pgType.has_value()) {
                                statement.DataFields.push_back({
                                    .Name = column.name(),
                                    .DataType = pgType->Oid,
                                    .DataTypeSize = pgType->Typlen,
                                    .DataTypeModifier = pgType->Typmod,
                                });
                            } else {
                                statement.DataFields.push_back({
                                    .Name = column.name()
                                });
                            }
                        }
                        break; // only 1 result is accepted
                    }
                    Send(Owner_, new TEvEvents::TEvUpdateStatement(statement));
                } else {
                    FillError(record, *Response_);
                }
            } else {
                Response_->ErrorFields.push_back({'E', "ERROR"});
                Response_->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            Response_->ErrorFields.push_back({'E', "ERROR"});
            Response_->ErrorFields.push_back({'M', e.what()});
        }
        return ReplyWithResponseAndPassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            hFunc(TEvEvents::TEvCancelRequest, TBase::Handle);
        }
    }
};

class TPgwireKqpProxyExecute : public TPgwireKqpProxy<TPgwireKqpProxyExecute, NPG::TEvPGEvents::TEvExecute> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyExecute, NPG::TEvPGEvents::TEvExecute>;

    TPortal Portal_;

public:
    TPgwireKqpProxyExecute(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvExecute::TPtr&& evExecute, const TPortal& portal)
        : TPgwireKqpProxy(owner, std::move(params), connection, std::move(evExecute))
        , Portal_(portal)
    {
    }

    void Bootstrap() {
        ResponseFormat_ = Portal_.BindData.ResultsFormat;
        auto event = ConvertQueryToRequest(Portal_.QueryData.Query);
        if (event) {
            for (unsigned int paramNum = 0; paramNum < Portal_.BindData.ParametersValue.size(); ++paramNum) {
                if (paramNum >= Portal_.ParameterTypes.size()) {
                    // TODO(xenoxeno): report error
                    break;
                }
                Ydb::Type type = Portal_.ParameterTypes[paramNum];
                int16_t format = 0; // text
                if (paramNum < Portal_.BindData.ParametersFormat.size()) {
                    format = Portal_.BindData.ParametersFormat[paramNum];
                }
                Ydb::TypedValue value = GetTypedValueFromParam(format, Portal_.BindData.ParametersValue[paramNum], type);
                event->Record.MutableRequest()->MutableYdbParameters()->insert({TStringBuilder() << "$p" << paramNum + 1, value});
            }
            SendToKQP(std::move(event));
        } else {
            return ReplyWithResponseAndPassAway();
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyExecute::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, TBase::Handle);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, TBase::Handle);
            hFunc(TEvEvents::TEvCancelRequest, Handle);
        }
    }
};

NActors::IActor* CreatePgwireKqpProxyQuery(const TActorId& owner,
                                           std::unordered_map<TString, TString> params,
                                           const TConnectionState& connection,
                                           TEvEvents::TEvSingleQuery::TPtr&& evQuery) {
    return new TPgwireKqpProxyQuery(owner, std::move(params), connection, std::move(evQuery));
}

NActors::IActor* CreatePgwireKqpProxyParse(const TActorId& owner,
                                           std::unordered_map<TString, TString> params,
                                           const TConnectionState& connection,
                                           NPG::TEvPGEvents::TEvParse::TPtr&& evParse) {
    return new TPgwireKqpProxyParse(owner, std::move(params), connection, std::move(evParse));
}

NActors::IActor* CreatePgwireKqpProxyExecute(const TActorId& owner,
                                             std::unordered_map<TString, TString> params,
                                             const TConnectionState& connection,
                                             NPG::TEvPGEvents::TEvExecute::TPtr&& evExecute,
                                             const TPortal& portal) {
    return new TPgwireKqpProxyExecute(owner, std::move(params), connection, std::move(evExecute), portal);
}

} //namespace NLocalPgwire
