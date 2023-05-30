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
    NActors::TActorId actorId,
    ui64 cookie,
    std::unordered_map<TString, TString> params
);

class TPgYdbConnection : public TActorBootstrapped<TPgYdbConnection> {
    using TBase = TActorBootstrapped<TPgYdbConnection>;

    std::unordered_map<TString, TString> ConnectionParams;
    std::unordered_map<TString, TParsedStatement> ParsedStatements;
    TString CurrentStatement;
    Ydb::StatusIds QueryStatus;
    std::unordered_map<ui32, NYdb::TResultSet> ResultSets;
public:
    TPgYdbConnection(std::unordered_map<TString, TString> params)
        : ConnectionParams(std::move(params))
    {}

    void Bootstrap() {
        Become(&TPgYdbConnection::StateWork);
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery " << ev->Sender);
        IActor* actor = CreatePgwireKqpProxy(ev->Sender, ev->Cookie, ConnectionParams);
        TActorId actorId = Register(actor);
        BLOG_D("Created pgwireKqpProxy: " << actorId);
        Forward(ev, actorId);
    }

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        BLOG_D("TEvParse " << ev->Sender);

        auto queryData = ev->Get()->Message->GetQueryData();
        ParsedStatements[queryData.Name].QueryData = queryData;

        auto parseComplete = ev->Get()->Reply();
        Send(ev->Sender, parseComplete.release());
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
            BLOG_W("TEvExecute changed empty statement to " << CurrentStatement);
        }
        auto it = ParsedStatements.find(statementName);
        if (it == ParsedStatements.end()) {
            auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
            errorResponse->ErrorFields.push_back({'E', "ERROR"});
            errorResponse->ErrorFields.push_back({'M', TStringBuilder() << "Parsed statement \"" << statementName << "\" not found"});
            Send(ev->Sender, errorResponse.release(), 0, ev->Cookie);
            return;
        }

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto query = ConvertQuery(it->second);
        Ydb::Scripting::ExecuteYqlRequest request;
        request.set_script(ToPgSyntax(query.Query, ConnectionParams));
        // TODO:
        //request.set_parameters(query.Params);
        TString database;
        if (ConnectionParams.count("database")) {
            database = ConnectionParams["database"];
        }
        TString token;
        if (ConnectionParams.count("ydb-serialized-token")) {
            token = ConnectionParams["ydb-serialized-token"];
        }
        using TRpcEv = NGRpcService::TGrpcRequestOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;
        auto rpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), database, token, actorSystem);
        // TODO: it's wrong. it should be done using explain to get all result meta. but it's not ready yet.
        rpcFuture.Subscribe([actorSystem, ev](NThreading::TFuture<Ydb::Scripting::ExecuteYqlResponse> future) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
            try {
                Ydb::Scripting::ExecuteYqlResponse yqlResponse(future.ExtractValueSync());
                if (yqlResponse.has_operation()) {
                    if (yqlResponse.operation().status() == Ydb::StatusIds::SUCCESS) {
                        if (yqlResponse.operation().has_result()) {
                            NYdb::NScripting::TExecuteYqlResult result(ConvertProtoResponseToSdkResult(std::move(yqlResponse)));
                            if (result.IsSuccess()) {
                                const TVector<NYdb::TResultSet>& resultSets = result.GetResultSets();
                                if (!resultSets.empty()) {
                                    NYdb::TResultSet resultSet = resultSets[0];

                                    {
                                        for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
                                            // TODO: fill data types and sizes
                                            response->DataFields.push_back({
                                                .Name = column.Name,
                                                .DataType = GetPgOidFromYdbType(column.Type),
                                            });
                                        }
                                    }
                                }
                            } else {
                                response->ErrorFields.push_back({'E', "ERROR"});
                                response->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)result});
                            }
                        }
                    } else {
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(yqlResponse.operation().issues(), issues);
                        NYdb::TStatus status(NYdb::EStatus(yqlResponse.operation().status()), std::move(issues));
                        response->ErrorFields.push_back({'E', "ERROR"});
                        response->ErrorFields.push_back({'M', TStringBuilder() << status});
                    }
                } else {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', "No result received"});
                }
            }
            catch (const std::exception& e) {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', e.what()});
            }

            actorSystem->Send(ev->Sender, response.release(), 0, ev->Cookie);
        });
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

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto query = ConvertQuery(it->second);
        Ydb::Scripting::ExecuteYqlRequest request;
        request.set_script(ToPgSyntax(query.Query, ConnectionParams));
        // TODO:
        //request.set_parameters(query.Params);
        TString database;
        if (ConnectionParams.count("database")) {
            database = ConnectionParams["database"];
        }
        TString token;
        if (ConnectionParams.count("ydb-serialized-token")) {
            token = ConnectionParams["ydb-serialized-token"];
        }
        using TRpcEv = NGRpcService::TGrpcRequestOperationCall<Ydb::Scripting::ExecuteYqlRequest, Ydb::Scripting::ExecuteYqlResponse>;
        auto rpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), database, token, actorSystem);

        rpcFuture.Subscribe([actorSystem, ev](NThreading::TFuture<Ydb::Scripting::ExecuteYqlResponse> future) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();
            try {
                Ydb::Scripting::ExecuteYqlResponse yqlResponse(future.ExtractValueSync());
                if (yqlResponse.has_operation()) {
                    if (yqlResponse.operation().status() == Ydb::StatusIds::SUCCESS) {
                        if (yqlResponse.operation().has_result()) {
                            NYdb::NScripting::TExecuteYqlResult result(ConvertProtoResponseToSdkResult(std::move(yqlResponse)));
                            if (result.IsSuccess()) {
                                const TVector<NYdb::TResultSet>& resultSets = result.GetResultSets();
                                if (!resultSets.empty()) {
                                    NYdb::TResultSet resultSet = resultSets[0];

                                    {
                                        auto maxRows = ev->Get()->Message->GetExecuteData().MaxRows;
                                        NYdb::TResultSetParser parser(std::move(resultSet));
                                        while (parser.TryNextRow()) {
                                            response->DataRows.emplace_back();
                                            auto& row = response->DataRows.back();
                                            row.resize(parser.ColumnsCount());
                                            for (size_t index = 0; index < parser.ColumnsCount(); ++index) {
                                                row[index] = ColumnValueToString(parser.ColumnParser(index));
                                            }
                                            if (maxRows != 0) {
                                                if (--maxRows == 0) {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            } else {
                                response->ErrorFields.push_back({'E', "ERROR"});
                                response->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)result});
                            }
                        }
                    } else {
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(yqlResponse.operation().issues(), issues);
                        NYdb::TStatus status(NYdb::EStatus(yqlResponse.operation().status()), std::move(issues));
                        response->ErrorFields.push_back({'E', "ERROR"});
                        response->ErrorFields.push_back({'M', TStringBuilder() << status});
                    }
                } else {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', "No result received"});
                }
            }
            catch (const std::exception& e) {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', e.what()});
            }

            actorSystem->Send(ev->Sender, response.release(), 0, ev->Cookie);
        });
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
