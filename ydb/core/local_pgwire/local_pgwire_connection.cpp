#include "log_impl.h"
#include "local_pgwire_util.h"

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
#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

extern NActors::IActor* CreatePgwireKqpProxy(
    std::unordered_map<TString, TString> params
);

NActors::IActor* CreatePgwireKqpProxyQuery(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvQuery::TPtr&& evQuery);
NActors::IActor* CreatePgwireKqpProxyParse(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvParse::TPtr&& evParse);
NActors::IActor* CreatePgwireKqpProxyDescribe(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvDescribe::TPtr&& evDescribe, const TParsedStatement& statement);
NActors::IActor* CreatePgwireKqpProxyExecute(const TActorId& owner, std::unordered_map<TString, TString> params, const TConnectionState& connection, NPG::TEvPGEvents::TEvExecute::TPtr&& evExecute, const TParsedStatement& statement);

class TPgYdbConnection : public TActor<TPgYdbConnection> {
    using TBase = TActor<TPgYdbConnection>;

    std::unordered_map<TString, TString> ConnectionParams;
    std::unordered_map<TString, TParsedStatement> ParsedStatements;
    TString CurrentStatement;
    TConnectionState Connection;
    std::deque<TAutoPtr<IEventHandle>> Events;
    ui32 Inflight = 0;

public:
    TPgYdbConnection(std::unordered_map<TString, TString> params)
        : TActor<TPgYdbConnection>(&TPgYdbConnection::StateSchedule)
        , ConnectionParams(std::move(params))
    {}

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery " << ev->Sender);
        if (IsQueryEmpty(ev->Get()->Message->GetQuery())) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
            response->EmptyQuery = true;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }
        ++Inflight;
        TActorId actorId = Register(CreatePgwireKqpProxyQuery(SelfId(), ConnectionParams, Connection, std::move(ev)));
        BLOG_D("Created pgwireKqpProxyQuery: " << actorId);
    }

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        BLOG_D("TEvParse " << ev->Sender);
        ++Inflight;
        TActorId actorId = Register(CreatePgwireKqpProxyParse(SelfId(), ConnectionParams, Connection, std::move(ev)));
        BLOG_D("Created pgwireKqpProxyParse: " << actorId);
        return;
    }

    void Handle(NPG::TEvPGEvents::TEvBind::TPtr& ev) {
        auto bindData = ev->Get()->Message->GetBindData();
        ParsedStatements[bindData.StatementName].BindData = bindData;
        CurrentStatement = bindData.StatementName;
        BLOG_D("TEvBind CurrentStatement changed to " << CurrentStatement);

        auto bindComplete = ev->Get()->Reply();
        Send(ev->Sender, bindComplete.release());
    }

    void Handle(NPG::TEvPGEvents::TEvClose::TPtr& ev) {
        auto closeData = ev->Get()->Message->GetCloseData();
        ParsedStatements.erase(closeData.StatementName);
        CurrentStatement.clear();
        BLOG_D("TEvClose CurrentStatement changed to <empty>");

        auto closeComplete = ev->Get()->Reply();
        Send(ev->Sender, closeComplete.release());
    }

    void Handle(NPG::TEvPGEvents::TEvDescribe::TPtr& ev) {
        BLOG_D("TEvDescribe " << ev->Sender);

        TString statementName = ev->Get()->Message->GetDescribeData().Name;
        if (statementName.empty()) {
            statementName = CurrentStatement;
            BLOG_W("TEvDescribe changed empty statement to " << CurrentStatement);
        }
        auto it = ParsedStatements.find(statementName);
        if (it == ParsedStatements.end()) {
            auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
            errorResponse->ErrorFields.push_back({'E', "ERROR"});
            errorResponse->ErrorFields.push_back({'M', TStringBuilder() << "Parsed statement \"" << statementName << "\" not found"});
            Send(ev->Sender, errorResponse.release(), 0, ev->Cookie);
            return;
        }

        auto response = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
        for (const auto& ydbType : it->second.ParameterTypes) {
            response->ParameterTypes.push_back(GetPgOidFromYdbType(ydbType));
        }
        response->DataFields = it->second.DataFields;

        Send(ev->Sender, response.release());
    }

    void Handle(NPG::TEvPGEvents::TEvExecute::TPtr& ev) {
        BLOG_D("TEvExecute " << ev->Sender);

        TString statementName = ev->Get()->Message->GetExecuteData().PortalName;
        if (statementName.empty()) {
            statementName = CurrentStatement;
            BLOG_W("TEvExecute changed empty statement to " << CurrentStatement);
        }
        auto it = ParsedStatements.find(statementName);
        if (it == ParsedStatements.end()) {
            auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();
            errorResponse->ErrorFields.push_back({'E', "ERROR"});
            errorResponse->ErrorFields.push_back({'M', TStringBuilder() << "Parsed statement \"" << statementName << "\" not found"});
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
        TActorId actorId = Register(CreatePgwireKqpProxyExecute(SelfId(), ConnectionParams, Connection, std::move(ev), it->second));
        BLOG_D("Created pgwireKqpProxyExecute: " << actorId);
    }

    void Handle(TEvEvents::TEvProxyCompleted::TPtr& ev) {
        --Inflight;
        BLOG_D("Received TEvProxyCompleted");
        if (ev->Get()->ParsedStatement) {
            ParsedStatements[ev->Get()->ParsedStatement.value().QueryData.Name] = ev->Get()->ParsedStatement.value();
        }
        if (ev->Get()->Connection) {
            auto& connection(ev->Get()->Connection.value());
            if (connection.Transaction.Status) {
                BLOG_D("Updating transaction state to " << connection.Transaction.Status);
                Connection.Transaction.Status = connection.Transaction.Status;
                switch (connection.Transaction.Status) {
                    case 'I':
                    case 'E':
                        Connection.Transaction.Id.clear();
                        BLOG_D("Transaction id cleared");
                        break;
                    case 'T':
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
        }
        while (!Events.empty() && Inflight == 0) {
            StateWork(Events.front());
            Events.pop_front();
        }
    }

    void PassAway() override {
        if (Connection.SessionId) {
            BLOG_D("Closing session " << Connection.SessionId);
            auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(Connection.SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        }
        TBase::PassAway();
    }

    STATEFN(StateSchedule) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvEvents::TEvProxyCompleted, Handle);
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
            hFunc(NPG::TEvPGEvents::TEvParse, Handle);
            hFunc(NPG::TEvPGEvents::TEvBind, Handle);
            hFunc(NPG::TEvPGEvents::TEvDescribe, Handle);
            hFunc(NPG::TEvPGEvents::TEvExecute, Handle);
            hFunc(NPG::TEvPGEvents::TEvClose, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};


NActors::IActor* CreateConnection(std::unordered_map<TString, TString> params) {
    return new TPgYdbConnection(std::move(params));
}

}
