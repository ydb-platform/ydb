#include "log_impl.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/pgproxy/pg_proxy_events.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

// temporary borrowed from postgresql/src/backend/catalog/pg_type_d.h

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

namespace NLocalPgWire {

using namespace NActors;
using namespace NKikimr;

class TPgYdbConnection : public TActorBootstrapped<TPgYdbConnection> {
    using TBase = TActorBootstrapped<TPgYdbConnection>;

    struct TParsedStatement {
        NPG::TPGParse::TQueryData QueryData;
        NPG::TPGBind::TBindData BindData;
    };

    std::unordered_map<TString, TString> ConnectionParams;
    std::unordered_map<TString, TParsedStatement> ParsedStatements;
    TString CurrentStatement;

public:
    TPgYdbConnection(std::unordered_map<TString, TString> params)
        : ConnectionParams(std::move(params))
    {}

    void Bootstrap() {
        Become(&TPgYdbConnection::StateWork);
    }

    static TString ColumnPrimitiveValueToString(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Bool:
                return TStringBuilder() << valueParser.GetBool();
            case NYdb::EPrimitiveType::Int8:
                return TStringBuilder() << valueParser.GetInt8();
            case NYdb::EPrimitiveType::Uint8:
                return TStringBuilder() << valueParser.GetUint8();
            case NYdb::EPrimitiveType::Int16:
                return TStringBuilder() << valueParser.GetInt16();
            case NYdb::EPrimitiveType::Uint16:
                return TStringBuilder() << valueParser.GetUint16();
            case NYdb::EPrimitiveType::Int32:
                return TStringBuilder() << valueParser.GetInt32();
            case NYdb::EPrimitiveType::Uint32:
                return TStringBuilder() << valueParser.GetUint32();
            case NYdb::EPrimitiveType::Int64:
                return TStringBuilder() << valueParser.GetInt64();
            case NYdb::EPrimitiveType::Uint64:
                return TStringBuilder() << valueParser.GetUint64();
            case NYdb::EPrimitiveType::Float:
                return TStringBuilder() << valueParser.GetFloat();
            case NYdb::EPrimitiveType::Double:
                return TStringBuilder() << valueParser.GetDouble();
            case NYdb::EPrimitiveType::Utf8:
                return TStringBuilder() << valueParser.GetUtf8();
            case NYdb::EPrimitiveType::Date:
                return valueParser.GetDate().ToString();
            case NYdb::EPrimitiveType::Datetime:
                return valueParser.GetDatetime().ToString();
            case NYdb::EPrimitiveType::Timestamp:
                return valueParser.GetTimestamp().ToString();
            case NYdb::EPrimitiveType::Interval:
                return TStringBuilder() << valueParser.GetInterval();
            case NYdb::EPrimitiveType::TzDate:
                return valueParser.GetTzDate();
            case NYdb::EPrimitiveType::TzDatetime:
                return valueParser.GetTzDatetime();
            case NYdb::EPrimitiveType::TzTimestamp:
                return valueParser.GetTzTimestamp();
            case NYdb::EPrimitiveType::String:
                return Base64Encode(valueParser.GetString());
            case NYdb::EPrimitiveType::Yson:
                return valueParser.GetYson();
            case NYdb::EPrimitiveType::Json:
                return valueParser.GetJson();
            case NYdb::EPrimitiveType::JsonDocument:
                return valueParser.GetJsonDocument();
            case NYdb::EPrimitiveType::DyNumber:
                return valueParser.GetDyNumber();
            case NYdb::EPrimitiveType::Uuid:
                return {};
        }
        return {};
    }

    static TString ColumnValueToString(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Primitive:
            return ColumnPrimitiveValueToString(valueParser);
        case NYdb::TTypeParser::ETypeKind::Optional: {
            TString value;
            valueParser.OpenOptional();
            if (valueParser.IsNull()) {
                value = "NULL";
            } else {
                value = ColumnValueToString(valueParser);
            }
            valueParser.CloseOptional();
            return value;
        }
        case NYdb::TTypeParser::ETypeKind::Tuple: {
            TString value;
            valueParser.OpenTuple();
            while (valueParser.TryNextElement()) {
                if (!value.empty()) {
                    value += ',';
                }
                value += ColumnValueToString(valueParser);
            }
            valueParser.CloseTuple();
            return value;
        }
        case NYdb::TTypeParser::ETypeKind::Pg: {
            return valueParser.GetPg().Content_;
        }
        default:
            return {};
        }
    }

    TString ToPgSyntax(TStringBuf query) {
        auto itOptions = ConnectionParams.find("options");
        if (itOptions == ConnectionParams.end()) {
            return TStringBuilder() << "--!syntax_pg\n" << query; // default
        }
        return TStringBuilder() << "--!" << itOptions->second << "\n" << query;
    }

    static NYdb::NScripting::TExecuteYqlResult ConvertProtoResponseToSdkResult(Ydb::Scripting::ExecuteYqlResponse&& proto) {
        TVector<NYdb::TResultSet> res;
        TMaybe<NYdb::NTable::TQueryStats> queryStats;
        {
            Ydb::Scripting::ExecuteYqlResult result;
            proto.mutable_operation()->mutable_result()->UnpackTo(&result);
            for (int i = 0; i < result.result_sets_size(); i++) {
                res.emplace_back(std::move(*result.mutable_result_sets(i)));
            }
            if (result.has_query_stats()) {
                queryStats = NYdb::NTable::TQueryStats(std::move(*result.mutable_query_stats()));
            }
        }
        NYdb::TPlainStatus alwaysSuccess;
        return {NYdb::TStatus(std::move(alwaysSuccess)), std::move(res), queryStats};
    }

    void Handle(NPG::TEvPGEvents::TEvQuery::TPtr& ev) {
        BLOG_D("TEvQuery " << ev->Sender);

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        Ydb::Scripting::ExecuteYqlRequest request;
        request.set_script(ToPgSyntax(ev->Get()->Message->GetQuery()));
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
            auto response = std::make_unique<NPG::TEvPGEvents::TEvQueryResponse>();
            // HACK
            if (ev->Get()->Message->GetQuery().starts_with("BEGIN")) {
                response->Tag = "BEGIN";
            }
            // HACK
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
                                            });
                                        }
                                    }

                                    {
                                        NYdb::TResultSetParser parser(std::move(resultSet));
                                        while (parser.TryNextRow()) {
                                            response->DataRows.emplace_back();
                                            auto& row = response->DataRows.back();
                                            row.resize(parser.ColumnsCount());
                                            for (size_t index = 0; index < parser.ColumnsCount(); ++index) {
                                                row[index] = ColumnValueToString(parser.ColumnParser(index));
                                            }
                                        }
                                    }

                                    // HACK
                                    response->Tag = TStringBuilder() << "SELECT " << response->DataRows.size();
                                    // HACK
                                }
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
                    paramsBuilder.AddParam(TStringBuilder() << ":_" << idxParam + 1).Int16(atoi(paramValue.data())).Build();
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

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto query = ConvertQuery(it->second);
        Ydb::Scripting::ExecuteYqlRequest request;
        request.set_script(ToPgSyntax(query.Query));
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

        TActorSystem* actorSystem = TActivationContext::ActorSystem();
        auto query = ConvertQuery(it->second);
        Ydb::Scripting::ExecuteYqlRequest request;
        request.set_script(ToPgSyntax(query.Query));
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
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};


NActors::IActor* CreateConnection(std::unordered_map<TString, TString> params) {
    return new TPgYdbConnection(std::move(params));
}

}
