#include "log_impl.h"
#include "local_pgwire_util.h"
#include "sql_parser.h"
#include "pgwire_kqp_proxy.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

class TPgYdbConnection : public TActorBootstrapped<TPgYdbConnection> {
    using TBase = TActorBootstrapped<TPgYdbConnection>;

    std::unordered_map<TString, TString> ConnectionParams;
    NPG::TEvPGEvents::TEvConnectionOpened::TPtr ConnectionEvent;
    std::unordered_map<TString, TParsedStatement> ParsedStatements;
    std::unordered_map<TString, TPortal> Portals;
    TConnectionState Connection;
    std::deque<TAutoPtr<IEventHandle>> Events;
    ui32 Inflight = 0;
    std::unordered_set<TActorId> CurrentRunningQueries;

public:
    TPgYdbConnection(std::unordered_map<TString, TString> params, NPG::TEvPGEvents::TEvConnectionOpened::TPtr&& event, const TConnectionState& connection)
        : ConnectionParams(std::move(params))
        , ConnectionEvent(std::move(event))
        , Connection(connection)
    {}

    void Bootstrap() {
        TString database;
        if (ConnectionParams.count("database")) {
            database = ConnectionParams["database"];
        }
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        auto& record = ev->Record;
        record.SetPgWire(true);
        NKikimrKqp::TCreateSessionRequest& request = *record.MutableRequest();
        if (ConnectionParams.count("application_name")) {
            record.SetApplicationName(ConnectionParams["application_name"]);
        }
        if (ConnectionParams.count("user")) {
            record.SetUserName(ConnectionParams["user"]);
        }
        request.SetDatabase(database);
        BLOG_D("Sent CreateSessionRequest to kqpProxy " << ev->Record.ShortDebugString());
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        TBase::Become(&TPgYdbConnection::StateCreateSession);
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        const auto& record(ev->Get()->Record);
        BLOG_D("Received TEvCreateSessionResponse " << record.ShortDebugString());
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            BLOG_D("Session id is " << record.GetResponse().GetSessionId());
            Connection.SessionId = record.GetResponse().GetSessionId();

            auto response = MakeHolder<NPG::TEvPGEvents::TEvFinishHandshake>();
            response->BackendData.Pid = SelfId().NodeId();
            response->BackendData.Key = Connection.ConnectionNum;
            Send(ConnectionEvent->Sender, response.Release(), 0, ev->Cookie);
            TBase::Become(&TPgYdbConnection::StateSchedule);
            ConnectionEvent.Destroy(); // don't need it anymore
        } else {
            BLOG_W("Failed to create session: " << record.ShortDebugString());
            auto response = MakeHolder<NPG::TEvPGEvents::TEvFinishHandshake>();
            // TODO: report actuall error
            response->ErrorFields.push_back({'E', "ERROR"});
            response->ErrorFields.push_back({'M', record.GetError()});
            //response->DropConnection = true; // it always closes connection on error on handshake
            Send(ConnectionEvent->Sender, response.Release(), 0, ev->Cookie);
            return PassAway();
        }
    }

    void ProcessEventsQueue() {
        while (!Events.empty() && Inflight == 0) {
            StateWork(Events.front());
            Events.pop_front();
        }
    }

    void Handle(TEvEvents::TEvSingleQuery::TPtr& ev) {
        BLOG_D("TEvSingleQuery " << ev->Sender);
        if (IsQueryEmpty(ev->Get()->Query)) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
            response->EmptyQuery = true;
            response->ReadyForQuery = ev->Get()->FinalQuery;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        ++Inflight;
        TActorId actorId = RegisterWithSameMailbox(CreatePgwireKqpProxyQuery(SelfId(), ConnectionParams, Connection, std::move(ev)));
        BLOG_D("Created pgwireKqpProxyQuery: " << actorId);
        CurrentRunningQueries.insert(actorId);
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery " << ev->Sender);

        TStatementIterator stmtIter((TString(ev->Get()->Message->GetQuery())));
        std::vector<TString> statements;

        for (auto pStmt = stmtIter.Next(); pStmt != nullptr; pStmt = stmtIter.Next()) {
            if (!statements.empty() && IsQueryEmpty(*pStmt)) {
                continue;
            }
            statements.push_back(*pStmt);
        }

        for (std::size_t n = 0; n < statements.size(); ++n) {
            Events.push_front(new NActors::IEventHandle(SelfId(), ev->Sender, new TEvEvents::TEvSingleQuery(statements[statements.size() - n - 1], n == 0), 0, ev->Cookie));
        }

        ProcessEventsQueue();
    }

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        BLOG_D("TEvParse " << ev->Sender);
        ++Inflight;
        TActorId actorId = RegisterWithSameMailbox(CreatePgwireKqpProxyParse(SelfId(), ConnectionParams, Connection, std::move(ev)));
        BLOG_D("Created pgwireKqpProxyParse: " << actorId);
        CurrentRunningQueries.insert(actorId);
    }

    void Handle(NPG::TEvPGEvents::TEvBind::TPtr& ev) {
        BLOG_D("TEvBind " << ev->Sender);
        auto bindData = ev->Get()->Message->GetBindData();
        auto statementName(bindData.StatementName);
        auto itParsedStatement = ParsedStatements.find(statementName);
        auto bindResponse = ev->Get()->Reply();
        if (itParsedStatement == ParsedStatements.end()) {
            bindResponse->ErrorFields.push_back({'E', "ERROR"});
            bindResponse->ErrorFields.push_back({'M', TStringBuilder() << "Parsed statement \"" << statementName << "\" not found"});
        } else {
            auto portalName(bindData.PortalName);
            // TODO(xenoxeno): performance hit
            Portals[portalName].Construct(itParsedStatement->second, std::move(bindData));
            BLOG_D("Created portal \"" << portalName << "\" from statement \"" << statementName <<"\"");
        }
        Send(ev->Sender, bindResponse.release(), 0, ev->Cookie);
    }

    void Handle(NPG::TEvPGEvents::TEvClose::TPtr& ev) {
        auto closeData = ev->Get()->Message->GetCloseData();
        switch (closeData.Type) {
            case NPG::TPGClose::TCloseData::ECloseType::Statement:
                ParsedStatements.erase(closeData.Name);
                break;
            case NPG::TPGClose::TCloseData::ECloseType::Portal:
                Portals.erase(closeData.Name);
                break;
            default:
                BLOG_ERROR("Unknown close type \"" << static_cast<char>(closeData.Type) << "\"");
                break;
        }
        auto closeComplete = ev->Get()->Reply();
        Send(ev->Sender, closeComplete.release());
    }

    void Handle(NPG::TEvPGEvents::TEvDescribe::TPtr& ev) {
        BLOG_D("TEvDescribe " << ev->Sender);
        auto response = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
        auto describeData = ev->Get()->Message->GetDescribeData();
        switch (describeData.Type) {
            case NPG::TPGDescribe::TDescribeData::EDescribeType::Statement: {
                auto it = ParsedStatements.find(describeData.Name);
                if (it == ParsedStatements.end()) {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', TStringBuilder() << "Parsed statement \"" << describeData.Name << "\" not found"});
                } else {
                    for (const auto& ydbType : it->second.ParameterTypes) {
                        response->ParameterTypes.push_back(GetPgOidFromYdbType(ydbType));
                    }
                    response->DataFields = it->second.DataFields;
                }
            }
            break;
            case NPG::TPGDescribe::TDescribeData::EDescribeType::Portal: {
                auto it = Portals.find(describeData.Name);
                if (it == Portals.end()) {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', TStringBuilder() << "Portal \"" << describeData.Name << "\" not found"});
                } else {
                    response->DataFields = it->second.DataFields;
                }
            }
            break;
            default: {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', TStringBuilder() << "Unknown describe type \"" << static_cast<char>(describeData.Type) << "\""});
            }
            break;
        }
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(NPG::TEvPGEvents::TEvExecute::TPtr& ev) {
        BLOG_D("TEvExecute " << ev->Sender);

        TString portalName = ev->Get()->Message->GetExecuteData().PortalName;
        auto it = Portals.find(portalName);
        if (it == Portals.end()) {
            auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();
            errorResponse->ErrorFields.push_back({'E', "ERROR"});
            errorResponse->ErrorFields.push_back({'M', TStringBuilder() << "Portal \"" << portalName << "\" not found"});
            Send(ev->Sender, errorResponse.release(), 0, ev->Cookie);
            return;
        }

        if (IsQueryEmpty(it->second.QueryData.Query)) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();
            response->EmptyQuery = true;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        ++Inflight;
        TActorId actorId = RegisterWithSameMailbox(CreatePgwireKqpProxyExecute(SelfId(), ConnectionParams, Connection, std::move(ev), it->second));
        BLOG_D("Created pgwireKqpProxyExecute: " << actorId);
        CurrentRunningQueries.insert(actorId);
    }

    void Handle(TEvEvents::TEvUpdateStatement::TPtr& ev) {
        auto name(ev->Get()->ParsedStatement.QueryData.Name);
        BLOG_D("Updating ParsedStatement \"" << name << "\"");
        ParsedStatements[name] = ev->Get()->ParsedStatement;
    }

    void Handle(TEvEvents::TEvProxyCompleted::TPtr& ev) {
        --Inflight;
        BLOG_D("Received TEvProxyCompleted");
        auto& connection(ev->Get()->Connection);
        if (connection.Transaction.Status) {
            BLOG_D("Updating transaction state to " << connection.Transaction.Status);
            Connection.Transaction.Status = connection.Transaction.Status;
            switch (connection.Transaction.Status) {
                case 'I':
                    Connection.Transaction.Id.clear();
                    BLOG_D("Transaction id cleared");
                    break;
                case 'T':
                case 'E':
                    if (connection.Transaction.Id) {
                        Connection.Transaction.Id = connection.Transaction.Id;
                        BLOG_D("Transaction id is " << Connection.Transaction.Id);
                    }
                    break;
            }
        }
        if (connection.SessionId) {
            BLOG_D("Session id is " << connection.SessionId);
            Connection.SessionId = connection.SessionId;
        }
        CurrentRunningQueries.erase(ev->Sender);
        ProcessEventsQueue();
    }

    void Handle(NPG::TEvPGEvents::TEvCancelRequest::TPtr&) {
        BLOG_D("Received TEvCancelRequest");
        for (const TActorId& actor : CurrentRunningQueries) {
            Send(actor, new TEvEvents::TEvCancelRequest());
        }
    }

    void PassAway() override {
        if (Connection.SessionId) {
            auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(Connection.SessionId);
            BLOG_D("Closing session " << Connection.SessionId << ", sent event to kqpProxy " << ev->Record.ShortDebugString());
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        }
        TBase::PassAway();
    }

    STATEFN(StateCreateSession) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateSchedule) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvEvents::TEvProxyCompleted, Handle);
            hFunc(TEvEvents::TEvUpdateStatement, Handle);
            hFunc(NPG::TEvPGEvents::TEvCancelRequest, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            default: {
                if (Inflight == 0) {
                    return StateWork(ev);
                } else {
                    Events.push_back(ev);
                }
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPG::TEvPGEvents::TEvQuery, Handle);
            hFunc(TEvEvents::TEvSingleQuery, Handle);
            hFunc(NPG::TEvPGEvents::TEvParse, Handle);
            hFunc(NPG::TEvPGEvents::TEvBind, Handle);
            hFunc(NPG::TEvPGEvents::TEvDescribe, Handle);
            hFunc(NPG::TEvPGEvents::TEvExecute, Handle);
            hFunc(NPG::TEvPGEvents::TEvClose, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};


NActors::IActor* CreateConnection(std::unordered_map<TString, TString> params, NPG::TEvPGEvents::TEvConnectionOpened::TPtr&& event, const TConnectionState& connection) {
    return new TPgYdbConnection(std::move(params), std::move(event), connection);
}

}
