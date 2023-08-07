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
#include <library/cpp/actors/core/actor_bootstrapped.h>


namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

template<typename Base>
class TPgwireKqpProxy : public TActorBootstrapped<Base> {
protected:
    using TBase = TActorBootstrapped<Base>;

    TActorId Owner_;
    std::unordered_map<TString, TString> ConnectionParams_;
    TConnectionState Connection_;
    TString Tag_;

    TPgwireKqpProxy(const TActorId owner, std::unordered_map<TString, TString> params, const TConnectionState& connection)
        : Owner_(owner)
        , ConnectionParams_(std::move(params))
        , Connection_(connection)
    {
        if (!Connection_.Transaction.Status) {
            Connection_.Transaction.Status = 'I';
        }
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
        event->Record.SetUserToken(token);
        return event;
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

    void ConvertQueryToRequest(TStringBuf query, NKikimrKqp::TQueryRequest& request) {
        if (Connection_.SessionId) {
            request.SetSessionId(Connection_.SessionId);
        }
        request.SetKeepSession(true);
        // HACK
        TString q(ToUpperASCII(query.substr(0, 10)));
        if (q.StartsWith("BEGIN")) {
            Tag_ = "BEGIN";
            request.SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
            request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        } else if (q.StartsWith("COMMIT")) {
            Tag_ = "COMMIT";
            request.SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
            request.MutableTxControl()->set_tx_id(Connection_.Transaction.Id);
        } else if (q.StartsWith("ROLLBACK")) {
            Tag_ = "ROLLBACK";
            if (Connection_.Transaction.Status == 'T') {
                request.SetAction(NKikimrKqp::QUERY_ACTION_ROLLBACK_TX);
                request.MutableTxControl()->set_tx_id(Connection_.Transaction.Id);
            } else if (Connection_.Transaction.Status == 'E') {
                // ignore, reset to I
                auto evQueryResponse = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
                evQueryResponse->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
                evQueryResponse->Record.GetRef().MutableResponse()->SetSessionId(request.GetSessionId());
                TBase::Send(TBase::SelfId(), evQueryResponse.Release());
            }
        } else {
            if (q.StartsWith("SELECT")) {
                Tag_ = "SELECT";
            }
            if (q.StartsWith("CREATE") || q.StartsWith("ALTER") || q.StartsWith("DROP")) {
                request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
            } else {
                request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
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
        }
    }

    void ProcessKqpResponseReleaseProxy(const NKikimrKqp::TEvQueryResponse& record) {
        Connection_.SessionId = record.GetResponse().GetSessionId();

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            Connection_.Transaction.Id = record.GetResponse().GetTxMeta().id();
            if (Connection_.Transaction.Id) {
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

        TBase::Send(Owner_, new TEvEvents::TEvProxyCompleted(Connection_));
    }

    void FillError(const NKikimrKqp::TEvQueryResponse& record, std::vector<std::pair<char, TString>>& errorFields) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        NYdb::TStatus status(NYdb::EStatus(record.GetYdbStatus()), std::move(issues));
        TString message(TStringBuilder() << status);
        errorFields.push_back({'E', "ERROR"});
        errorFields.push_back({'M', message});
        if (message.find("Error: Cannot find table") != TString::npos) {
            errorFields.push_back({'C', "42P01"});
        }
    }
};

class TPgwireKqpProxyQuery : public TPgwireKqpProxy<TPgwireKqpProxyQuery> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyQuery>;

    TEvEvents::TEvSingleQuery::TPtr EventQuery_;
    bool WasMeta_ = false;
    std::size_t RowsSelected_ = 0;

public:
    TPgwireKqpProxyQuery(const TActorId& owner,
                         std::unordered_map<TString, TString> params,
                         const TConnectionState& connection,
                         TEvEvents::TEvSingleQuery::TPtr&& evQuery)
        : TPgwireKqpProxy(owner, std::move(params), connection)
        , EventQuery_(std::move(evQuery))
    {
    }

    void Bootstrap() {
        auto query(EventQuery_->Get()->Query);
        auto event = MakeKqpRequest();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();

        // HACK
        ConvertQueryToRequest(query, request);
        if (request.HasAction()) {
            ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
            BLOG_D("Sent event to kqpProxy " << request.ShortDebugString());
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyQuery::StateWork);
    }

    void FillMeta(const NYdb::TResultSet& resultSet, NPG::TEvPGEvents::TEvQueryResponse* response) {
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

    std::unique_ptr<NPG::TEvPGEvents::TEvQueryResponse> MakeResponse() {
        auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();

        response->Tag = Tag_;
        response->TransactionStatus = Connection_.Transaction.Status;

        return response;
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        NYdb::TResultSet resultSet(std::move(*ev->Get()->Record.MutableResultSet()));
        auto response = MakeResponse();
        if (!WasMeta_) {
            FillMeta(resultSet, response.get());
            WasMeta_ = true;
        }
        FillResultSet(resultSet, response.get()->DataRows);
        response->CommandCompleted = false;
        response->ReadyForQuery = false;

        RowsSelected_ += response->DataRows.size();

        BLOG_D(this->SelfId() << " Send rowset " << ev->Get()->Record.GetQueryResultIndex() << " data " << ev->Get()->Record.GetSeqNo() << " to " << EventQuery_->Sender);
        Send(EventQuery_->Sender, response.release(), 0, EventQuery_->Cookie);

        BLOG_D(this->SelfId() << " Send stream data ack to " << ev->Sender);
        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(std::numeric_limits<ui64>::max());
        Send(ev->Sender, resp.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse " << ev->Get()->Record.ShortDebugString());
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        ProcessKqpResponseReleaseProxy(record);
        auto response = MakeResponse();
        try {
            if (record.HasYdbStatus()) {
                if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    BLOG_ENSURE(record.GetResponse().GetResults().empty());

                    // HACK
                    if (response->Tag == "SELECT") {
                        response->Tag = TStringBuilder() << response->Tag << " " << RowsSelected_;
                    }
                } else {
                    FillError(record, response->ErrorFields);
                }
            } else {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            response->ErrorFields.push_back({'E', "ERROR"});
            response->ErrorFields.push_back({'M', e.what()});
        }
        response->CommandCompleted = true;
        response->ReadyForQuery = EventQuery_->Get()->FinalQuery;
        BLOG_D("Finally replying to " << EventQuery_->Sender);
        Send(EventQuery_->Sender, response.release(), 0, EventQuery_->Cookie);
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
        }
    }
};

class TPgwireKqpProxyParse : public TPgwireKqpProxy<TPgwireKqpProxyParse> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyParse>;

    NPG::TEvPGEvents::TEvParse::TPtr EventParse_;
    NPG::TPGParse::TQueryData QueryData_;

public:
    TPgwireKqpProxyParse(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvParse::TPtr&& evParse)
        : TPgwireKqpProxy(owner, std::move(params), connection)
        , EventParse_(std::move(evParse))
        , QueryData_(EventParse_->Get()->Message->GetQueryData())
    {}

    void Bootstrap() {
        auto event = MakeKqpRequest();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();

        // HACK
        ConvertQueryToRequest(QueryData_.Query, request);
        if (request.HasAction()) {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);

            request.SetUsePublicResponseDataFormat(true);

            ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
            BLOG_D("Sent event to kqpProxy " << request.ShortDebugString());
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyParse::StateWork);
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse " << ev->Get()->Record.ShortDebugString());
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        auto response = EventParse_->Get()->Reply();
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
                    Send(Owner_, new TEvEvents::TEvProxyCompleted(statement));
                } else {
                    FillError(record, response->ErrorFields);
                }
            } else {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            response->ErrorFields.push_back({'E', "ERROR"});
            response->ErrorFields.push_back({'M', e.what()});
        }
        if (!response->ErrorFields.empty()) {
            Send(Owner_, new TEvEvents::TEvProxyCompleted());
        }
        BLOG_D("Finally replying to " << EventParse_->Sender);
        Send(EventParse_->Sender, response.release(), 0, EventParse_->Cookie);
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        }
    }
};

class TPgwireKqpProxyExecute : public TPgwireKqpProxy<TPgwireKqpProxyExecute> {
    using TBase = TPgwireKqpProxy<TPgwireKqpProxyExecute>;

    NPG::TEvPGEvents::TEvExecute::TPtr EventExecute_;
    TParsedStatement Statement_;
    std::size_t RowsSelected_ = 0;

public:
    TPgwireKqpProxyExecute(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvExecute::TPtr&& evExecute, const TParsedStatement& statement)
        : TPgwireKqpProxy(owner, std::move(params), connection)
        , EventExecute_(std::move(evExecute))
        , Statement_(statement)
    {
    }

    void Bootstrap() {
        auto event = MakeKqpRequest();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();

        // HACK
        ConvertQueryToRequest(Statement_.QueryData.Query, request);
        if (request.HasAction()) {
            for (unsigned int paramNum = 0; paramNum < Statement_.BindData.ParametersValue.size(); ++paramNum) {
                if (paramNum >= Statement_.ParameterTypes.size()) {
                    // TODO(xenoxeno): report error
                    break;
                }
                Ydb::Type type = Statement_.ParameterTypes[paramNum];
                int16_t format = 0; // text
                if (paramNum < Statement_.BindData.ParametersFormat.size()) {
                    format = Statement_.BindData.ParametersFormat[paramNum];
                }
                Ydb::TypedValue value = GetTypedValueFromParam(format, Statement_.BindData.ParametersValue[paramNum], type);
                request.MutableYdbParameters()->insert({TStringBuilder() << "$p" << paramNum + 1, value});
            }
            ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
            BLOG_D("Sent event to kqpProxy " << request.ShortDebugString());
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
        }
        // TODO(xenoxeno): timeout
        Become(&TPgwireKqpProxyExecute::StateWork);
    }

    std::unique_ptr<NPG::TEvPGEvents::TEvExecuteResponse> MakeResponse() {
        auto response = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();

        response->Tag = Tag_;
        response->TransactionStatus = Connection_.Transaction.Status;

        return response;
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        NYdb::TResultSet resultSet(std::move(*ev->Get()->Record.MutableResultSet()));
        auto response = MakeResponse();
        FillResultSet(resultSet, response.get()->DataRows, Statement_.BindData.ResultsFormat);
        response->CommandCompleted = false;
        response->ReadyForQuery = false;

        RowsSelected_ += response->DataRows.size();

        BLOG_D(this->SelfId() << " Send rowset " << ev->Get()->Record.GetQueryResultIndex() << " data " << ev->Get()->Record.GetSeqNo() << " to " << EventExecute_->Sender);
        Send(EventExecute_->Sender, response.release(), 0, EventExecute_->Cookie);

        BLOG_D(this->SelfId() << " Send stream data ack to " << ev->Sender);
        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(std::numeric_limits<ui64>::max());
        Send(ev->Sender, resp.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_D("Handling TEvKqp::TEvQueryResponse " << ev->Get()->Record.ShortDebugString());
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        ProcessKqpResponseReleaseProxy(record);
        auto response = MakeResponse();
        try {
            if (record.HasYdbStatus()) {
                if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    BLOG_ENSURE(record.GetResponse().GetResults().empty());

                    // HACK
                    if (response->Tag == "SELECT") {
                        response->Tag = TStringBuilder() << response->Tag << " " << RowsSelected_;
                    }
                } else {
                    FillError(record, response->ErrorFields);
                }
            } else {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', "No result received"});
            }
        } catch (const std::exception& e) {
            response->ErrorFields.push_back({'E', "ERROR"});
            response->ErrorFields.push_back({'M', e.what()});
        }
        response->CommandCompleted = true;
        BLOG_D("Finally replying to " << EventExecute_->Sender);
        Send(EventExecute_->Sender, response.release(), 0, EventExecute_->Cookie);
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
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
                                             const TParsedStatement& statement) {
    return new TPgwireKqpProxyExecute(owner, std::move(params), connection, std::move(evExecute), statement);
}

} //namespace NLocalPgwire
