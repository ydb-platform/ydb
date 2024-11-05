#pragma once

#include "cli.h"
#include "cli_command.h"
#include "cli_grpc.h"
#include "cli_kicli.h"

#include <ydb/public/lib/ydb_cli/common/root.h>

#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

namespace NKikimr {
namespace NDriverClient {

class TClientCommandRootKikimrBase : public TClientCommandRootBase {
public:
    TClientCommandRootKikimrBase(const TString& name);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    void ParseCredentials(TConfig& config) override;

protected:
    bool GetProfileVariable(const TString& name, TString& value);

private:
    void ParseProfile();

    THolder<TProfileConfig> ProfileConfig;
    TString LocalProfileName;
    TString UserName;
    TString PasswordFile;
    bool DoNotAskForPassword = false;
    bool DumpRequests = false;
};

class TClientCommandSchemaLite : public TClientCommandTree {
public:
    TClientCommandSchemaLite();
};

class TClientCommandDb : public TClientCommandTree {
public:
    TClientCommandDb();
};

class TClientCommandDiscoveryBase
    : public TClientGRpcCommand<Ydb::Discovery::V1::DiscoveryService,
    Ydb::Discovery::ListEndpointsRequest,
    Ydb::Discovery::ListEndpointsResponse,
    decltype(&Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints),
    &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints> {
public:
    TClientCommandDiscoveryBase(const TString& name, const TString& description);
    int Run(TConfig &config) override;
    void PrintResponse(const Ydb::Operations::Operation &response) override;

protected:
    TString Database;
};

// kikimr
class TClientCommandDiscovery : public TClientCommandTree {
public:
    TClientCommandDiscovery();
};

// ydb
class TClientCommandDiscoveryLite : public TClientCommandDiscoveryBase {
public:
    TClientCommandDiscoveryLite();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
};

}
}
