#pragma once

#include "cli_command.h"

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <util/string/split.h>
#include <util/string/type.h>

namespace NKikimr {
namespace NDriverClient {

template <typename TService, typename TRequest, typename TResponse, typename TFunction>
int DoGRpcRequest(const NGRpcProxy::TGRpcClientConfig &clientConfig,
                  const TRequest &request,
                  Ydb::Operations::Operation &response,
                  TFunction function,
                  const TString& securityToken)
{
    int res = 0;

    if (!clientConfig.Locator) {
        Cerr << "GRPC call error: GRPC server is not specified (MBus protocol is not supported for this command)." << Endl;
        return -2;
    }

    NYdbGrpc::TCallMeta meta;
    if (securityToken) {
        meta.Aux.push_back({NYdb::YDB_AUTH_TICKET_HEADER, securityToken});
    }

    NYdbGrpc::TResponseCallback<TResponse> responseCb =
        [&res, &response](NYdbGrpc::TGrpcStatus &&grpcStatus, TResponse &&resp) -> void {
        res = (int)grpcStatus.GRpcStatusCode;
        if (!res) {
            response.CopyFrom(resp.operation());
        } else {
            Cerr << "GRPC call error: " << grpcStatus.Msg << Endl;
        }
    };

    {
        NYdbGrpc::TGRpcClientLow clientLow;
        auto connection = clientLow.CreateGRpcServiceConnection<TService>(clientConfig);
        connection->DoRequest(request, responseCb, function, meta);
    }

    NYdbGrpc::TResponseCallback<Ydb::Operations::GetOperationResponse> operationCb =
        [&res, &response](NYdbGrpc::TGrpcStatus &&grpcStatus, Ydb::Operations::GetOperationResponse &&resp) -> void {
        res = (int)grpcStatus.GRpcStatusCode;
        if (!res) {
            response.CopyFrom(resp.operation());
        } else {
            Cerr << "GRPC call error: " << grpcStatus.Msg << Endl;
        }
    };

    while (!res && !response.ready()) {
        Sleep(TDuration::MilliSeconds(100));
        NYdbGrpc::TGRpcClientLow clientLow;
        auto connection = clientLow.CreateGRpcServiceConnection<Ydb::Operation::V1::OperationService>(clientConfig);
        Ydb::Operations::GetOperationRequest request;
        request.set_id(response.id());
        connection->DoRequest(request, operationCb, &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation, meta);
    }

    return res;
}

template <typename TService, typename TRequest, typename TResponse,
          typename TFunction, TFunction function>
class TClientGRpcCommand : public TClientCommand {
public:
    TRequest GRpcRequest;
    NGRpcProxy::TGRpcClientConfig ClientConfig;

    TClientGRpcCommand(const TString &name,
                             const std::initializer_list<TString> &aliases,
                             const TString &description)
        : TClientCommand(name, aliases, description)
    {}

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);

        ClientConfig.Locator = CommandConfig.ClientConfig.Locator;
        ClientConfig.Timeout = CommandConfig.ClientConfig.Timeout;
        ClientConfig.MaxMessageSize = CommandConfig.ClientConfig.MaxMessageSize;
        ClientConfig.MaxInFlight = CommandConfig.ClientConfig.MaxInFlight;
        ClientConfig.EnableSsl = CommandConfig.ClientConfig.EnableSsl;
        ClientConfig.SslCredentials.pem_root_certs = CommandConfig.ClientConfig.SslCredentials.pem_root_certs;
    }

    static int PrepareConfigCredentials(NGRpcProxy::TGRpcClientConfig clientConfig, TConfig& commandConfig) {
        int res = grpc::StatusCode::OK;

        if (!commandConfig.StaticCredentials.User.empty()) {
            Ydb::Auth::LoginRequest request;
            Ydb::Operations::Operation response;
            request.set_user(commandConfig.StaticCredentials.User);
            request.set_password(commandConfig.StaticCredentials.Password);
            res = DoGRpcRequest<Ydb::Auth::V1::AuthService,
                                Ydb::Auth::LoginRequest,
                                Ydb::Auth::LoginResponse>(clientConfig, request, response, &Ydb::Auth::V1::AuthService::Stub::AsyncLogin, {});
            if (res == grpc::StatusCode::OK && response.status() == Ydb::StatusIds::SUCCESS) {
                Ydb::Auth::LoginResult result;
                if (response.result().UnpackTo(&result)) {
                    commandConfig.SecurityToken = result.token();
                }
            } else {
                Cerr << response.status() << Endl;
                for (auto &issue : response.issues()) {
                    Cerr << issue.message() << Endl;
                }
                if (res == grpc::StatusCode::OK) {
                    res = -2;
                }
            }
        }
        return res;
    }

    int Run(TConfig& config) override
    {
        Ydb::Operations::Operation response;
        int res;

        res = PrepareConfigCredentials(ClientConfig, config);
        if (res != grpc::StatusCode::OK) {
            return res;
        }

        res = DoGRpcRequest<TService, TRequest, TResponse, TFunction>
            (ClientConfig, GRpcRequest, response, function, config.SecurityToken);

        if (!res) {
            PrintResponse(response);
        }

        return res;
    }

    virtual void PrintResponse(const Ydb::Operations::Operation &response)
    {
        if (response.status() == Ydb::StatusIds::SUCCESS) {
            Cout << "OK" << Endl;
        } else {
            Cout << "ERROR: " << response.status() << Endl;
            for (auto &issue : response.issues())
                Cout << issue.message() << Endl;
        }
    }
};

}
}

