#include "utils.h"
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

namespace NYq {

using namespace NYdb::NYq;

void UpdateConnections(
    TClient& client,
    const TString& folderId,
    const TString& connectionsStr) {
    NJson::TJsonValue value;
    TStringStream in(connectionsStr);
    NJson::ReadJsonTree(&in, &value);
    YandexQuery::ListConnectionsRequest listConsRequest;
    listConsRequest.set_limit(100);
    auto result = client
        .ListConnections(listConsRequest, CreateYqSettings<TListConnectionsSettings>(folderId))
        .ExtractValueSync();
    NYdb::NConsoleClient::ThrowOnError(result);
    THashMap<TString, YandexQuery::Connection> connections;
    for (const auto& connection : result.GetResult().connection()) {
        if (!connection.content().name()) {
            continue;
        }
        connections.emplace(connection.content().name(), connection);
    }
    for (auto conJson : value.GetArraySafe()) {
        const auto& content = conJson["content"];
        const TString conName = content["name"].GetString();
        auto it = connections.find(conName);
        if (it == connections.end()) {
            Cerr << " Add " << conName << Endl;
            YandexQuery::CreateConnectionRequest request;
            NProtobufJson::Json2Proto(conJson, request);
            const auto result = client
                .CreateConnection(request, CreateYqSettings<TCreateConnectionSettings>(folderId))
                .ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            NYdb::NConsoleClient::ThrowOnError(result);
        } else {
            Cerr << " Update " << conName << Endl;
            YandexQuery::ModifyConnectionRequest request;
            conJson["connection_id"] = it->second.meta().id();
            NProtobufJson::Json2Proto(conJson, request);
            const auto result = client
                .ModifyConnection(request, CreateYqSettings<TModifyConnectionSettings>(folderId))
                .ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            NYdb::NConsoleClient::ThrowOnError(result);
        }
    }
}

} //NYq
