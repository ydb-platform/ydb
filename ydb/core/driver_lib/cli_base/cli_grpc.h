#pragma once

#include "cli_command.h"

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <library/cpp/grpc/client/grpc_client_low.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
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

    NGrpc::TCallMeta meta;
    if (securityToken) {
        meta.Aux.push_back({NYdb::YDB_AUTH_TICKET_HEADER, securityToken});
    }

    NGrpc::TResponseCallback<TResponse> responseCb =
        [&res, &response](NGrpc::TGrpcStatus &&grpcStatus, TResponse &&resp) -> void {
        res = (int)grpcStatus.GRpcStatusCode;
        if (!res) {
            response.CopyFrom(resp.operation());
        } else {
            Cerr << "GRPC call error: " << grpcStatus.Msg << Endl;
        }
    };

    {
        NGrpc::TGRpcClientLow clientLow;
        auto connection = clientLow.CreateGRpcServiceConnection<TService>(clientConfig);
        connection->DoRequest(request, responseCb, function, meta);
    }

    NGrpc::TResponseCallback<Ydb::Operations::GetOperationResponse> operationCb =
        [&res, &response](NGrpc::TGrpcStatus &&grpcStatus, Ydb::Operations::GetOperationResponse &&resp) -> void {
        res = (int)grpcStatus.GRpcStatusCode;
        if (!res) {
            response.CopyFrom(resp.operation());
        } else {
            Cerr << "GRPC call error: " << grpcStatus.Msg << Endl;
        }
    };

    while (!res && !response.ready()) {
        Sleep(TDuration::MilliSeconds(100));
        NGrpc::TGRpcClientLow clientLow;
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

        if (CommandConfig.ClientConfig.Defined()) {
            auto *p = std::get_if<NGRpcProxy::TGRpcClientConfig>(&CommandConfig.ClientConfig.GetRef());
            if (p) {
                ClientConfig.Locator = p->Locator;
                ClientConfig.Timeout = p->Timeout;
                ClientConfig.MaxMessageSize = p->MaxMessageSize;
                ClientConfig.MaxInFlight = p->MaxInFlight;
                ClientConfig.EnableSsl = p->EnableSsl;
                ClientConfig.SslCredentials.pem_root_certs = p->SslCredentials.pem_root_certs;
            }
        }
    }

    int Run(TConfig& config) override
    {
        Ydb::Operations::Operation response;
        int res;

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

