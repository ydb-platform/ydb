#include "pg_ydb_proxy.h"
#include "pg_ydb_connection.h"
#include "log_impl.h"
#include <ydb/core/pgproxy/pg_proxy_events.h>
#include <ydb/core/local_pgwire/local_pgwire_util.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

// temporarry borrowed from postgresql/src/backend/catalog/pg_type_d.h

#define BOOLOID 16
#define BYTEAOID 17
#define CHAROID 18
#define NAMEOID 19
#define INT8OID 20
#define INT2OID 21
#define INT2VECTOROID 22
#define INT4OID 23
#define REGPROCOID 24
#define TEXTOID 25

//

namespace NPGW {

using namespace NActors;

class TPgYdbConnection : public TActorBootstrapped<TPgYdbConnection> {
    using TBase = TActorBootstrapped<TPgYdbConnection>;

    struct TEvPrivate {
        enum EEv {
            EvParseComplete = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        struct TEvParseComplete : TEventLocal<TEvParseComplete, EvParseComplete> {
            TString Name;
            NYdb::NTable::TDataQuery DataQuery;

            TEvParseComplete(const TString& name, NYdb::NTable::TDataQuery&& dataQuery)
                : Name(name)
                , DataQuery(std::move(dataQuery))
            {}
        };
    };

    struct TParsedStatement {
        NPG::TPGParse::TQueryData QueryData;
        NPG::TPGBind::TBindData BindData;
    };

    NYdb::TDriver Driver;
    std::unordered_map<TString, TString> ConnectionParams;
    std::unordered_map<TString, TParsedStatement> ParsedStatements;
    TString CurrentStatement;
    std::unique_ptr<NYdb::NScripting::TScriptingClient> ScriptingClient;
    //std::unique_ptr<NYdb::NTable::TTableClient> TableClient;

public:
    TPgYdbConnection(NYdb::TDriver driver, std::unordered_map<TString, TString> params)
        : Driver(std::move(driver))
        , ConnectionParams(std::move(params))
    {}

    void Bootstrap() {
        Become(&TPgYdbConnection::StateWork);
    }

    static TString ToPgSyntax(TStringBuf query) {
        return TStringBuilder() << "--!syntax_pg\n" << query;
    }

    NYdb::NTable::TClientSettings GetClientSettings() {
        NYdb::NTable::TClientSettings clientSettings;
        if (ConnectionParams.count("database")) {
            clientSettings.Database(ConnectionParams["database"]);
        }
        if (ConnectionParams.count("auth-token")) {
            clientSettings.AuthToken(ConnectionParams["auth-token"]);
        }
        return clientSettings;
    }

    void BootstrapScriptingClient() {
        ScriptingClient = std::make_unique<NYdb::NScripting::TScriptingClient>(Driver, GetClientSettings());
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery " << ev->Sender);

        if (!ScriptingClient) {
            BootstrapScriptingClient();
        }

        TActorSystem* actorSystem = TActivationContext::ActorSystem();

        ScriptingClient->ExecuteYqlScript(ToPgSyntax(ev->Get()->Message->GetQuery())).Subscribe([actorSystem, ev](const NYdb::NScripting::TAsyncExecuteYqlResult& feature) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
            try {
                NYdb::NScripting::TExecuteYqlResult result = NYdb::NScripting::TAsyncExecuteYqlResult(feature).ExtractValue();
                if (result.IsSuccess()) {
                    const TVector<NYdb::TResultSet>& resultSets = result.GetResultSets();
                    if (!resultSets.empty()) {
                        NYdb::TResultSet resultSet = resultSets[0];

                        {
                            for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
                                std::optional<NYdb::TPgType> pgType = NLocalPgWire::GetPgTypeFromYdbType(column.Type);
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

                        {
                            NYdb::TResultSetParser parser(std::move(resultSet));
                            while (parser.TryNextRow()) {
                                response->DataRows.emplace_back();
                                auto& row = response->DataRows.back();
                                row.resize(parser.ColumnsCount());
                                for (size_t index = 0; index < parser.ColumnsCount(); ++index) {
                                    row[index] = NLocalPgWire::ColumnValueToRowValueField(parser.ColumnParser(index));
                                }
                            }
                        }
                    }
                } else {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)result});
                }
            }
            catch (const std::exception& e) {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', e.what()});
            }

            actorSystem->Send(ev->Sender, response.release(), 0, ev->Cookie);
        });
    }

    /*void BootstrapTableClient() {
        TableClient = std::make_unique<NYdb::NTable::TTableClient>(Driver, GetClientSettings());
    }*/

    void Handle(NPG::TEvPGEvents::TEvParse::TPtr& ev) {
        BLOG_D("TEvParse " << ev->Sender);

        /*if (!TableClient) {
            BootstrapTableClient();
        }

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        TActorId selfId = SelfId();

        TableClient->RetryOperation([actorSystem, ev, selfId](NYdb::NTable::TSession session) {
            auto queryData = ev->Get()->Message->GetQueryData();
            const TString& query = queryData.Query;
            return session.PrepareDataQuery(query).Apply([actorSystem, ev, selfId, queryData](NYdb::NTable::TAsyncPrepareQueryResult result) mutable -> NThreading::TFuture<NYdb::TStatus> {
                NYdb::NTable::TPrepareQueryResult res = result.ExtractValue();

                if (res.IsSuccess()) {
                    auto privateParseComplete = std::make_unique<TEvPrivate::TEvParseComplete>(queryData.Name, res.GetQuery());
                    actorSystem->Send(selfId, privateParseComplete.release());
                    auto parseComplete = std::make_unique<NPG::TEvPGEvents::TEvParseComplete>();
                    parseComplete->OriginalMessage = std::move(ev->Get()->Message);
                    actorSystem->Send(ev->Sender, parseComplete.release());
                } else {
                    auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvErrorResponse>();
                    errorResponse->ErrorFields.push_back({'E', "ERROR"});
                    errorResponse->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)res});
                    actorSystem->Send(ev->Sender, errorResponse.release());
                }
                return NThreading::MakeFuture<NYdb::TStatus>(res);
            });
        });*/

        /*auto result = ParsedQueries.emplace(std::piecewise_construct, std::make_tuple(ev->Get()->Name), std::make_tuple(ev->Get()->DataQuery));
        if (!result.second) {
            result.first->second.DataQuery = std::move(ev->Get()->DataQuery);
        }*/
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
        ParsedStatements.erase(closeData.Name);
        CurrentStatement.clear();
        BLOG_D("TEvClose CurrentStatement changed to <empty>");

        auto closeComplete = ev->Get()->Reply();
        Send(ev->Sender, closeComplete.release());
    }

    struct TConvertedQuery {
        TString Query;
        NYdb::TParams Params;
    };

    TConvertedQuery ConvertQuery(const TParsedStatement& statement) {
        auto& bindData = statement.BindData;
        const auto& queryData = statement.QueryData;
        NYdb::TParamsBuilder paramsBuilder;
        TStringBuilder injectedQuery;

        for (size_t idxParam = 0; idxParam < queryData.ParametersTypes.size(); ++idxParam) {
            int32_t paramType = queryData.ParametersTypes[idxParam];
            TString paramValue;
            if (idxParam < bindData.ParametersValue.size()) {
                std::vector<uint8_t> paramVal = bindData.ParametersValue[idxParam];
                paramValue = TString(reinterpret_cast<char*>(paramVal.data()), paramVal.size());
            }
            switch (paramType) {
                case INT2OID:
                    paramsBuilder.AddParam(TStringBuilder() << "$_" << idxParam + 1).Int16(atoi(paramValue.data())).Build();
                    injectedQuery << "DECLARE $_" << idxParam + 1 << " AS Int16;" << Endl;
                    break;

            }
        }
        return {
            .Query = injectedQuery + queryData.Query,
            .Params = paramsBuilder.Build(),
        };
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

        if (!ScriptingClient) {
            BootstrapScriptingClient();
        }

        auto query = ConvertQuery(it->second);
        TActorSystem* actorSystem = TActivationContext::ActorSystem();

        // TODO: it's wrong. it should be done using explain to get all result meta. but it's not ready yet.

        ScriptingClient->ExecuteYqlScript(query.Query, query.Params).Subscribe([actorSystem, ev](const NYdb::NScripting::TAsyncExecuteYqlResult& feature) {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvDescribeResponse>();
            try {
                NYdb::NScripting::TExecuteYqlResult result = NYdb::NScripting::TAsyncExecuteYqlResult(feature).ExtractValue();
                if (result.IsSuccess()) {
                    const TVector<NYdb::TResultSet>& resultSets = result.GetResultSets();
                    if (!resultSets.empty()) {
                        NYdb::TResultSet resultSet = resultSets[0];

                        {
                            for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
                                std::optional<NYdb::TPgType> pgType = NLocalPgWire::GetPgTypeFromYdbType(column.Type);
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
                    }
                } else {
                    response->ErrorFields.push_back({'E', "ERROR"});
                    response->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)result});
                }
            }
            catch (const std::exception& e) {
                response->ErrorFields.push_back({'E', "ERROR"});
                response->ErrorFields.push_back({'M', e.what()});
            }

            actorSystem->Send(ev->Sender, response.release(), 0, ev->Cookie);
        });

        /*
        TableClient->RetryOperation([actorSystem, sender, dataQuery](NYdb::NTable::TSession session) mutable {
            session.
            return dataQuery.Execute(NYdb::NTable::TTxControl::BeginTx()).Apply([actorSystem, sender](NYdb::NTable::TAsyncDataQueryResult result) mutable -> NThreading::TFuture<NYdb::TStatus> {
                NYdb::NTable::TDataQueryResult res = result.ExtractValue();

                if (res.IsSuccess()) {

                    const TVector<NYdb::TResultSet>& resultSets = res.GetResultSets();
                    if (!resultSets.empty()) {
                        NYdb::TResultSet resultSet = resultSets[0];

                        {
                            auto rowDescription = std::make_unique<NPG::TEvPGEvents::TEvRowDescription>();
                            for (const NYdb::TColumn& column : resultSet.GetColumnsMeta()) {
                                // TODO: fill data types and sizes
                                rowDescription->Fields.push_back({
                                    .Name = column.Name,
                                });
                            }

                            actorSystem->Send(sender, rowDescription.release());
                        }
                    }
                } else {
                    auto errorResponse = std::make_unique<NPG::TEvPGEvents::TEvErrorResponse>();
                    errorResponse->ErrorFields.push_back({'E', "ERROR"});
                    errorResponse->ErrorFields.push_back({'M', TStringBuilder() << (NYdb::TStatus&)res});
                    actorSystem->Send(sender, errorResponse.release());
                }
                return NThreading::MakeFuture<NYdb::TStatus>(res);
            });
        });*/
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

        if (!ScriptingClient) {
            BootstrapScriptingClient();
        }

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto query = ConvertQuery(it->second);

        ScriptingClient->ExecuteYqlScript(query.Query, query.Params).Subscribe([actorSystem, ev](const NYdb::NScripting::TAsyncExecuteYqlResult& feature) mutable {
            auto response = std::make_unique<NPG::TEvPGEvents::TEvExecuteResponse>();
            try {
                NYdb::NScripting::TExecuteYqlResult result = NYdb::NScripting::TAsyncExecuteYqlResult(feature).ExtractValue();
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
                                    row[index] = NLocalPgWire::ColumnValueToRowValueField(parser.ColumnParser(index));
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


NActors::IActor* CreateConnection(NYdb::TDriver driver, std::unordered_map<TString, TString> params) {
    return new TPgYdbConnection(std::move(driver), std::move(params));
}

}
