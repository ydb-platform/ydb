#include "ydb_service_discovery.h"

namespace NYdb {
namespace NConsoleClient {

TCommandDiscovery::TCommandDiscovery()
    : TClientCommandTree("discovery", {}, "Discovery service operations")
{
    AddCommand(std::make_unique<TCommandListEndpoints>());
    AddCommand(std::make_unique<TCommandWhoAmI>());
}

TCommandListEndpoints::TCommandListEndpoints()
    : TYdbSimpleCommand("list", {}, "List endpoints")
{}

void TCommandListEndpoints::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);
    config.Opts->AddLongOption('p', "piles", "Output piles info").StoreTrue(&OutputPilesInfo).DefaultValue(false).Hidden();
    config.SetFreeArgsNum(0);
}

int TCommandListEndpoints::Run(TConfig& config) {
    NDiscovery::TDiscoveryClient client(CreateDriver(config));
    NDiscovery::TListEndpointsResult result = client.ListEndpoints(
        FillSettings(NDiscovery::TListEndpointsSettings())
    ).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    PrintResponse(result);
    return EXIT_SUCCESS;
}

void TCommandListEndpoints::PrintResponse(NDiscovery::TListEndpointsResult& result) {
    const std::vector<NDiscovery::TEndpointInfo>& endpoints = result.GetEndpointsInfo();
    if (endpoints.size()) {
        for (auto& endpoint : endpoints) {
            if (endpoint.Ssl) {
                Cout << "grpcs://";
            } else {
                Cout << "grpc://";
            }
            Cout << endpoint.Address << ":" << endpoint.Port;
            if (!endpoint.Location.empty()) {
                Cout << " [" << endpoint.Location << "]";
            }
            if (!endpoint.BridgePileName.empty()) {
                Cout << " (" << endpoint.BridgePileName << ")";
            }
            for (const auto& service : endpoint.Services) {
                Cout << " #" << service;
            }
            Cout << Endl;
        }
    } else {
        Cout << "Endpoint list Is empty." << Endl;
    }

    const auto& pileStates = result.GetPileStates();
    if (OutputPilesInfo && pileStates.size()) {
        Cout << Endl;
        for (const auto& pileState : pileStates) {
            Cout << "Pile \"" << pileState.PileName << "\": " << pileState.State << Endl;
        }
    }
}

TCommandWhoAmI::TCommandWhoAmI()
    : TYdbSimpleCommand("whoami", {}, "Who am I?")
{}

void TCommandWhoAmI::Config(TConfig& config) {
    TYdbSimpleCommand::Config(config);
    config.Opts->AddLongOption('g', "groups", "With groups").StoreTrue(&WithGroups);
    config.Opts->AddLongOption('a', "access-levels", "With access levels").StoreTrue(&WithAccessLevels);
    config.SetFreeArgsNum(0);
}

int TCommandWhoAmI::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    NDiscovery::TDiscoveryClient client(driver);
    NDiscovery::TWhoAmIResult result = client.WhoAmI(
        FillSettings(NDiscovery::TWhoAmISettings().WithGroups(WithGroups))
    ).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    PrintResponse(result);
    driver.Stop(true);
    return EXIT_SUCCESS;
}

void TCommandWhoAmI::PrintResponse(NDiscovery::TWhoAmIResult& result) {
    const std::string& userName = result.GetUserName();
    if (!userName.empty()) {
        Cout << "User SID: " << userName << Endl;
        if (WithGroups) {
            const std::vector<std::string>& groups = result.GetGroups();
            if (groups.size() > 0) {
                Cout << Endl << "Group SIDs:" << Endl;
                for (const std::string& group : groups) {
                    Cout << group << Endl;
                }
            } else {
                Cout << Endl << "User has no groups" << Endl;
            }

            if (WithAccessLevels) {
                bool hasAnyAccess = result.IsDatabaseAllowed() || result.IsViewerAllowed() ||
                    result.IsMonitoringAllowed() || result.IsAdministrationAllowed() ||
                    result.IsRegisterNodeAllowed() || result.IsBootstrapAllowed();
                if (hasAnyAccess) {
                    Cout << Endl << "Access levels:" << Endl;
                    if (result.IsDatabaseAllowed()) Cout << "Database" << Endl;
                    if (result.IsViewerAllowed()) Cout << "Viewer" << Endl;
                    if (result.IsMonitoringAllowed()) Cout << "Monitoring" << Endl;
                    if (result.IsAdministrationAllowed()) Cout << "Administration" << Endl;
                    if (result.IsRegisterNodeAllowed()) Cout << "Register node" << Endl;
                    if (result.IsBootstrapAllowed()) Cout << "Bootstrap" << Endl;
                }
            }
        }
    }
}

}
}
