#include "cli.h"
#include "cli_cmds.h"


#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/core/protos/console_base.pb.h>

#include <util/string/split.h>
#include <util/string/join.h>
#include <util/string/type.h>

namespace NKikimr {
namespace NDriverClient {

class TTenantClientCommand : public TClientCommand {
public:
    TString Domain;
    NKikimrClient::TConsoleRequest Request;

    TTenantClientCommand(const TString &name,
                         const std::initializer_list<TString> &aliases,
                         const TString &description)
        : TClientCommand(name, aliases, description)
    {
    }

    void Config(TConfig &config) override
    {
        TClientCommand::Config(config);

        Domain = "";

        config.Opts->AddLongOption("domain", "Set target domain (required for clusters with multiple domains)")
            .RequiredArgument("NAME").StoreResult(&Domain);
    }

    void Parse(TConfig& config) override
    {
        TClientCommand::Parse(config);

        if (Domain)
            Request.SetDomainName(Domain);
    }

    int Run(TConfig &config) override
    {
        TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest);
        request->Record.CopyFrom(Request);

        auto handler = [this](const NMsgBusProxy::TBusConsoleResponse &response) -> int {
            PrintResponse(response.Record);
            return 0;
        };

        int result = MessageBusCall<NMsgBusProxy::TBusConsoleRequest,
                                    NMsgBusProxy::TBusConsoleResponse>(config, request, handler);
        return result;
    }

    virtual void PrintResponse(const NKikimrClient::TConsoleResponse &response)
    {
        if (response.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS)
            Cout << response.DebugString();
        else
            Cout << "ERROR: " << response.GetStatus().GetCode()
                 << " (" << response.GetStatus().GetReason() << ")" << Endl;
    }

    virtual void PrintResponse(const Ydb::Operations::Operation &response)
    {
        if (response.status() == Ydb::StatusIds::SUCCESS)
            Cout << "OK" << Endl;
        else {
            Cout << "ERROR: " << response.status() << Endl;
            for (auto &issue : response.issues())
                Cout << issue.message() << Endl;
        }
    }

    void ParseStoragePools(TConfig &config, TVector<std::pair<TString, ui64>> &pools)
    {
        for (auto &arg : config.ParseResult->GetFreeArgs()) {
            TVector<TString> items = StringSplitter(arg).Split(':').ToList<TString>();
            if (items.size() != 2)
                ythrow yexception() << "bad format in '" + arg + "'";
            ui64 size = FromString<ui64>(items[1]);
            if (!size)
                ythrow yexception() << "bad pool size '" + arg + "'";
            pools.push_back(std::make_pair(items[0], size));
        }
    }

    void ParseUnits(TConfig &config, THashMap<std::pair<TString, TString>, ui64> &units)
    {
        for (auto &arg : config.ParseResult->GetFreeArgs()) {
            TString kind;
            TString zone;
            ui64 count = 0;

            TVector<TString> items = StringSplitter(arg).Split(':').ToList<TString>();
            if (items.size() > 3)
                ythrow yexception() << "bad format in '" + arg + "'";
            auto it = items.rbegin();
            count = FromString<ui64>(*it++);
            if (it != items.rend()) {
                kind = *it++;
                if (it != items.rend()) {
                    zone = *it;
                }
            }
            units[std::make_pair(kind, zone)] += count;
        }
    }
};

template <typename TService, typename TRequest, typename TResponse,
          typename TFunction, TFunction function>
class TTenantClientGRpcCommand : public TTenantClientCommand {
public:
    TRequest GRpcRequest;
    NYdbGrpc::TGRpcClientConfig ClientConfig;

    TTenantClientGRpcCommand(const TString &name,
                             const std::initializer_list<TString> &aliases,
                             const TString &description)
        : TTenantClientCommand(name, aliases, description)
    {
    }

    void Parse(TConfig& config) override
    {
        TTenantClientCommand::Parse(config);

        ClientConfig.Locator = CommandConfig.ClientConfig.Locator;
        ClientConfig.Timeout = CommandConfig.ClientConfig.Timeout;
        ClientConfig.MaxMessageSize = CommandConfig.ClientConfig.MaxMessageSize;
        ClientConfig.MaxInFlight = CommandConfig.ClientConfig.MaxInFlight;
        ClientConfig.EnableSsl = CommandConfig.ClientConfig.EnableSsl;
        ClientConfig.SslCredentials.pem_root_certs = CommandConfig.ClientConfig.SslCredentials.pem_root_certs;
    }

    int Run(TConfig &config) override
    {
        Ydb::Operations::Operation response;
        int res;

        res = TClientGRpcCommand<TService, TRequest, TResponse, TFunction, function>::PrepareConfigCredentials(ClientConfig, config);
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
};

class TClientCommandTenantList
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::ListDatabasesRequest,
                                      Ydb::Cms::ListDatabasesResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncListDatabases),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncListDatabases> {
public:
    TClientCommandTenantList()
        : TTenantClientGRpcCommand("list", {}, "List existing databases")
    {}

    using TTenantClientGRpcCommand::PrintResponse;

    void PrintResponse(const Ydb::Operations::Operation &response) override
    {
        if (response.status() != Ydb::StatusIds::SUCCESS) {
            TTenantClientGRpcCommand::PrintResponse(response);
        } else {
            Ydb::Cms::ListDatabasesResult result;
            Y_ABORT_UNLESS(response.result().UnpackTo(&result));

            Cout << "Databases:" << Endl;
            for (auto &path : result.paths())
                Cout << "  " << path << Endl;
        }
    }
};

class TClientCommandTenantOptions
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::DescribeDatabaseOptionsRequest,
                                      Ydb::Cms::DescribeDatabaseOptionsResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncDescribeDatabaseOptions),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncDescribeDatabaseOptions> {
public:
    TClientCommandTenantOptions()
        : TTenantClientGRpcCommand("options", {}, "Describe available database options")
    {}

    using TTenantClientGRpcCommand::PrintResponse;

    void PrintResponse(const Ydb::Operations::Operation &response) override
    {
        if (response.status() != Ydb::StatusIds::SUCCESS) {
            TTenantClientGRpcCommand::PrintResponse(response);
        } else {
            Ydb::Cms::DescribeDatabaseOptionsResult result;
            Y_ABORT_UNLESS(response.result().UnpackTo(&result));

            Cout << "Storage units" << Endl;
            for (auto &unit : result.storage_units()) {
                Cout << " - " << unit.kind() << Endl;
                for (auto &pr : unit.labels())
                    Cout << "     " << pr.first << ": " << pr.second << Endl;
            }
            Cout << "Availability zones" << Endl;
            for (auto &zone : result.availability_zones()) {
                Cout << " - " << zone.name() << Endl;
                for (auto &pr : zone.labels())
                    Cout << "     " << pr.first << ": " << pr.second << Endl;
            }
            Cout << "Computational units" << Endl;
            for (auto &unit : result.computational_units()) {
                Cout << " - " << unit.kind() << Endl;
                Cout << "     Allowed zones: " << JoinSeq(", ", unit.allowed_availability_zones()) << Endl;
                for (auto &pr : unit.labels())
                    Cout << "     " << pr.first << ": " << pr.second << Endl;
            }
        }
    }
};

class TClientCommandTenantStatus
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::GetDatabaseStatusRequest,
                                      Ydb::Cms::GetDatabaseStatusResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncGetDatabaseStatus),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncGetDatabaseStatus> {
public:
    TClientCommandTenantStatus()
        : TTenantClientGRpcCommand("status", {}, "Get database status")
    {}

    void Config(TConfig &config) override
    {
        TTenantClientGRpcCommand::Config(config);
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        GRpcRequest.set_path(config.Tenant);
    }

    using TTenantClientGRpcCommand::PrintResponse;

    void PrintResponse(const Ydb::Operations::Operation &response) override
    {
        if (response.status() != Ydb::StatusIds::SUCCESS) {
            TTenantClientGRpcCommand::PrintResponse(response);
        } else {
            Ydb::Cms::GetDatabaseStatusResult result;
            Y_ABORT_UNLESS(response.result().UnpackTo(&result));
            // type -> <required, allocated>
            THashMap<TString, std::pair<ui64, ui64>> pools;
            // <type, dc> -> <required, allocated>
            THashMap<std::pair<TString, TString>, std::pair<ui64, ui64>> units;

            for (auto &unit : result.required_resources().storage_units())
                pools[unit.unit_kind()] = std::make_pair(unit.count(), 0U);

            for (auto &unit : result.required_resources().computational_units()) {
                auto key = std::make_pair(unit.unit_kind(), unit.availability_zone());
                units[key] = std::make_pair(unit.count(), 0ULL);
            }

            for (auto &unit : result.allocated_resources().storage_units())
                pools[unit.unit_kind()].second = unit.count();

            for (auto &unit : result.allocated_resources().computational_units()) {
                auto key = std::make_pair(unit.unit_kind(), unit.availability_zone());
                units[key].second = unit.count();
            }

            Cout << "Database " << result.path() << " status:" << Endl
                 << "  State: " << result.state() << Endl;
            Cout << "  Allocated pools:" << Endl;
            for (auto &pr : pools)
                Cout << "    " << pr.first << ": "
                     << pr.second.second << "/" << pr.second.first << Endl;
            Cout << "  Allocated units:" << Endl;
            for (auto &pr : units)
                Cout << "    [" << (pr.first.first ? pr.first.first : "ANY") << ":"
                     << (pr.first.second ? ToString(pr.first.second) : "ANY") << "]: "
                     << pr.second.second << "/" << pr.second.first << Endl;
            Cout << "  Registered units:" << Endl;
            for (auto &unit : result.registered_resources()) {
                Cout << "    " << unit.host() << ":" << unit.port() << " - " << unit.unit_kind() << Endl;
            }
            Cout << "  Data size hard quota: " << result.database_quotas().data_size_hard_quota() << Endl;
            Cout << "  Data size soft quota: " << result.database_quotas().data_size_soft_quota() << Endl;
        }
    }
};

class TClientCommandTenantQuotasBase {
    uint64_t DataSizeHardQuota;
    uint64_t DataSizeSoftQuota;
public:
    void Config(auto& config) {
        config.Opts->AddLongOption("data-size-hard-quota", "A maximum data size in bytes, new data will be rejected when exceeded")
            .OptionalArgument("NUM").StoreResult(&DataSizeHardQuota);
        config.Opts->AddLongOption("data-size-soft-quota", "Data size in bytes (lower than data_size_hard_quota), at this value new data ingestion is re-enabled again")
            .OptionalArgument("NUM").StoreResult(&DataSizeSoftQuota);
    }

    void Parse(auto& config, auto& gRpcRequest)
    {
        if (config.ParseResult->Has("data-size-hard-quota"))
            gRpcRequest.mutable_database_quotas()->set_data_size_hard_quota(DataSizeHardQuota);
        if (config.ParseResult->Has("data-size-soft-quota"))
            gRpcRequest.mutable_database_quotas()->set_data_size_soft_quota(DataSizeSoftQuota);
    }
};

class TClientCommandTenantCreate
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::CreateDatabaseRequest,
                                      Ydb::Cms::CreateDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncCreateDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncCreateDatabase>,
                                      TClientCommandTenantQuotasBase {
    TVector<TString> Attributes;
    bool Shared = false;
    bool Serverless = false;

public:
    TClientCommandTenantCreate()
        : TTenantClientGRpcCommand("create", {}, "Create new database")
    {}

    void Config(TConfig &config) override
    {
        TTenantClientGRpcCommand::Config(config);
        TClientCommandTenantQuotasBase::Config(config);

        config.Opts->AddLongOption("no-tx", "Disable tenant services for database")
            .NoArgument();
        config.Opts->AddLongOption("attr", "Attach attribute name=value to database")
            .RequiredArgument("NAME=VALUE").AppendTo(&Attributes);
        config.Opts->AddLongOption("shared", "Create a shared database")
            .NoArgument().StoreTrue(&Shared);
        config.Opts->AddLongOption("serverless", "Create a serverless database (free arg must specify shared database for resources)")
            .NoArgument().StoreTrue(&Serverless);
        config.SetFreeArgsMin(1);
        config.Opts->SetFreeArgDefaultTitle("<pool type>:<pool size>", "Pairs describing storage pool type and size (number of groups).");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);
        TClientCommandTenantQuotasBase::Parse(config, GRpcRequest);

        GRpcRequest.set_path(config.Tenant);
        if (config.ParseResult->Has("no-tx"))
            GRpcRequest.mutable_options()->set_disable_tx_service(true);

        for (auto &attr : Attributes) {
            TVector<TString> items = StringSplitter(attr).Split('=').ToList<TString>();
            if (items.size() != 2)
                ythrow yexception() << "bad format in attr '" + attr + "'";
            (*GRpcRequest.mutable_attributes())[items[0]] = items[1];
        }

        Ydb::Cms::Resources* resources;

        if (Shared) {
            resources = GRpcRequest.mutable_shared_resources();
        } else {
            resources = GRpcRequest.mutable_resources();
        }
        if (Serverless) {
            const auto& args = config.ParseResult->GetFreeArgs();
            if (args.size() != 1)
                ythrow yexception() << "exactly one free arg must specify a path to shared database";
            GRpcRequest.mutable_serverless_resources()->set_shared_database_path(args[0]);
            return;
        }

        TVector<std::pair<TString, ui64>> pools;
        ParseStoragePools(config, pools);
        for (auto &pool : pools) {
            auto &protoPool = *resources->add_storage_units();
            protoPool.set_unit_kind(pool.first);
            protoPool.set_count(pool.second);
        }
    }
};

class TClientCommandTenantRemove
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::RemoveDatabaseRequest,
                                      Ydb::Cms::RemoveDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncRemoveDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncRemoveDatabase> {
private:
    Ydb::Cms::RemoveDatabaseRequest Request;
    bool Force;

public:
    TClientCommandTenantRemove()
        : TTenantClientGRpcCommand("remove", {}, "Remove database")
        , Force(false)
    {}

    void Config(TConfig &config) override
    {
        TTenantClientGRpcCommand::Config(config);
        config.Opts->AddLongOption("force", "Force command execution")
            .NoArgument().SetFlag(&Force);
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        GRpcRequest.set_path(config.Tenant);
    }

    int Run(TConfig &config) override
    {
        TString answer;
        while (!Force && answer != "yes") {
            Cout << "!!WARNING!! This action will completely remove database with all data. Continue? [yes/no] ";
            Cin >> answer;
            if (answer == "no")
                return 1;
        }

        return TTenantClientGRpcCommand::Run(config);
    }
};

class TClientCommandTenantAddUnits
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase> {
public:
    TClientCommandTenantAddUnits()
        : TTenantClientGRpcCommand("add", {}, "Add computational units for database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("[[<availability zone>:]<unit kind>:]<units count>", "Triples describing units.");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        // <type, zone> -> count
        THashMap<std::pair<TString, TString>, ui64> units;
        ParseUnits(config, units);

        GRpcRequest.set_path(config.Tenant);
        for (auto &pr : units) {
            auto &unit = *GRpcRequest.add_computational_units_to_add();
            unit.set_unit_kind(pr.first.first);
            unit.set_availability_zone(pr.first.second);
            unit.set_count(pr.second);
        }
    }
};

class TClientCommandTenantRemoveUnits
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase> {
public:
    TClientCommandTenantRemoveUnits()
        : TTenantClientGRpcCommand("remove", {}, "Remove computational units from database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("[[<availability zone>:]<unit kind>:]<units count>", "Triples describing units.");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        // <type, zone> -> count
        THashMap<std::pair<TString, TString>, ui64> units;
        ParseUnits(config, units);

        GRpcRequest.set_path(config.Tenant);
        for (auto &pr : units) {
            auto &unit = *GRpcRequest.add_computational_units_to_remove();
            unit.set_unit_kind(pr.first.first);
            unit.set_availability_zone(pr.first.second);
            unit.set_count(pr.second);
        }
    }
};

class TClientCommandTenantRegisterUnits
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase> {
public:
    TClientCommandTenantRegisterUnits()
        : TTenantClientGRpcCommand("register", {}, "Register computational units for database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("<host>:<port>:<kind>", "Triples describing registered units.");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        GRpcRequest.set_path(config.Tenant);
        for (auto &arg : config.ParseResult->GetFreeArgs()) {
            TString host;
            ui32 port;
            TString kind;

            TVector<TString> items = StringSplitter(arg).Split(':').ToList<TString>();
            if (items.size() != 3)
                ythrow yexception() << "bad format in '" + arg + "'";
            if (!TryFromString(items[1], port))
                ythrow yexception() << "bad port in '" + arg + "'";

            auto &unit = *GRpcRequest.add_computational_units_to_register();
            unit.set_host(items[0]);
            unit.set_port(port);
            unit.set_unit_kind(items[2]);
        }
    }
};

class TClientCommandTenantDeregisterUnits
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase> {
public:
    TClientCommandTenantDeregisterUnits()
        : TTenantClientGRpcCommand("deregister", {}, "Deregister computational units for database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        config.SetFreeArgsMin(0);
        config.Opts->SetFreeArgDefaultTitle("<host>:<port>", "Pairs describing deregistered units.");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        GRpcRequest.set_path(config.Tenant);
        for (auto &arg : config.ParseResult->GetFreeArgs()) {
            TString host;
            ui32 port;

            TVector<TString> items = StringSplitter(arg).Split(':').ToList<TString>();
            if (items.size() != 2)
                ythrow yexception() << "bad format in '" + arg + "'";
            if (!TryFromString(items[1], port))
                ythrow yexception() << "bad port in '" + arg + "'";

            auto &unit = *GRpcRequest.add_computational_units_to_deregister();
            unit.set_host(items[0]);
            unit.set_port(port);
        }
    }
};

class TClientCommandTenantAddPools
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase> {
public:
    TClientCommandTenantAddPools()
        : TTenantClientGRpcCommand("add", {}, "Add storage resources for database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        config.SetFreeArgsMin(1);
        config.Opts->SetFreeArgDefaultTitle("<pool kind>:<pool size>", "Pairs describing storage pool type and size (number of groups).");
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);

        TVector<std::pair<TString, ui64>> pools;
        ParseStoragePools(config, pools);

        GRpcRequest.set_path(config.Tenant);
        for (auto &pool : pools) {
            auto &unit = *GRpcRequest.add_storage_units_to_add();
            unit.set_unit_kind(pool.first);
            unit.set_count(pool.second);
        }
    }
};

class TClientCommandTenantChangeQuotas
    : public TTenantClientGRpcCommand<Ydb::Cms::V1::CmsService,
                                      Ydb::Cms::AlterDatabaseRequest,
                                      Ydb::Cms::AlterDatabaseResponse,
                                      decltype(&Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase),
                                      &Ydb::Cms::V1::CmsService::Stub::AsyncAlterDatabase>,
                                      TClientCommandTenantQuotasBase {
public:
    TClientCommandTenantChangeQuotas()
        : TTenantClientGRpcCommand("change", {}, "Change data size quotas for database")
    {}

    void Config(TConfig& config) override {
        TTenantClientGRpcCommand::Config(config);
        TClientCommandTenantQuotasBase::Config(config);
        config.SetFreeArgsMin(0);
    }

    void Parse(TConfig& config) override
    {
        TTenantClientGRpcCommand::Parse(config);
        TClientCommandTenantQuotasBase::Parse(config, GRpcRequest);
        GRpcRequest.set_path(config.Tenant);
    }
};

class TClientCommandTenantUnits : public TClientCommandTree {
public:
    TClientCommandTenantUnits()
        : TClientCommandTree("units", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandTenantAddUnits>());
        AddCommand(std::make_unique<TClientCommandTenantRemoveUnits>());
        AddCommand(std::make_unique<TClientCommandTenantRegisterUnits>());
        AddCommand(std::make_unique<TClientCommandTenantDeregisterUnits>());
    }
};

class TClientCommandTenantPools : public TClientCommandTree {
public:
    TClientCommandTenantPools()
        : TClientCommandTree("pools", {}, "")
    {
        AddCommand(std::make_unique<TClientCommandTenantAddPools>());
    }
};

class TClientCommandTenantQuotas : public TClientCommandTree {
public:
    TClientCommandTenantQuotas()
        : TClientCommandTree("quotas", {}, "Manage database size quotas")
    {
        AddCommand(std::make_unique<TClientCommandTenantChangeQuotas>());
    }
};

class TClientCommandTenantName : public TClientCommandTree {
public:
    TClientCommandTenantName()
        : TClientCommandTree("*", {}, "<database name>")
    {
        AddCommand(std::make_unique<TClientCommandTenantCreate>());
        AddCommand(std::make_unique<TClientCommandTenantPools>());
        AddCommand(std::make_unique<TClientCommandTenantRemove>());
        AddCommand(std::make_unique<TClientCommandTenantStatus>());
        AddCommand(std::make_unique<TClientCommandTenantUnits>());
        AddCommand(std::make_unique<TClientCommandTenantQuotas>());
    }

    virtual void Parse(TConfig& config) override {
        config.Tenant = config.Tokens.back();

        TClientCommandTree::Parse(config);
    }
};

TClientCommandTenant::TClientCommandTenant()
    : TClientCommandTree("database", {"db", "tenant"}, "Database administration")
{
    AddCommand(std::make_unique<TClientCommandTenantName>());
    AddCommand(std::make_unique<TClientCommandTenantList>());
    AddCommand(std::make_unique<TClientCommandTenantOptions>());
}

}
}
