#include "cli_cmds.h"

namespace NKikimr {
namespace NDriverClient {

TClientCommandDiscoveryBase::TClientCommandDiscoveryBase(const TString& name, const TString& description)
    : TClientGRpcCommand(name, {}, description)
{}

int TClientCommandDiscoveryBase::Run(TConfig &config) {
    GRpcRequest.set_database(Database);
    return TClientGRpcCommand::Run(config);
}

//using TClientGRpcCommand::PrintResponse;
void TClientCommandDiscoveryBase::PrintResponse(const Ydb::Operations::Operation &response) {
    if (response.status() != Ydb::StatusIds::SUCCESS) {
        TClientGRpcCommand::PrintResponse(response);
    } else {
        Ydb::Discovery::ListEndpointsResult result;
        Y_ABORT_UNLESS(response.result().UnpackTo(&result));
        Cout << "OK [" << result.Getself_location() << "]" << Endl;
        for (auto &endpoint : result.Getendpoints()) {
            Cout << (endpoint.Getssl() ? "grpcs://" : "grpc://");
            Cout << endpoint.Getaddress() << ":" << endpoint.Getport();
            if (endpoint.Getlocation())
                Cout << " [" << endpoint.Getlocation() << "]";
            if (endpoint.serviceSize()) {
                for (auto &x : endpoint.Getservice())
                    Cout << " #" << x;
            }
            Cout << Endl;
        }
    }
}

// Old kikimr behavior:
// ./kikimr discovery list -d <database>
struct TClientCommandDiscoveryListEndpoints
    : TClientCommandDiscoveryBase
{
    TClientCommandDiscoveryListEndpoints()
        : TClientCommandDiscoveryBase("list", "List existing tenants")
    {}

    void Config(TConfig &config) override {
        TClientCommand::Config(config);
        Database = "";
        config.Opts->AddLongOption('d', "db", "Set target database")
            .RequiredArgument("NAME").StoreResult(&Database);
    }
};

TClientCommandDiscovery::TClientCommandDiscovery()
    : TClientCommandTree("discovery", {}, "Endpoint discovery")
{
    AddCommand(std::make_unique<TClientCommandDiscoveryListEndpoints>());
}

// New YDB behavior:
// ./ydb discover <database>

TClientCommandDiscoveryLite::TClientCommandDiscoveryLite()
    : TClientCommandDiscoveryBase("discover", "Endpoint discovery")
{}

void TClientCommandDiscoveryLite::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<database>", "Database to discover");
}

void TClientCommandDiscoveryLite::Parse(TConfig& config) {
    TClientGRpcCommand::Parse(config);
    Database = config.ParseResult->GetFreeArgs().at(0);
}

}
}
