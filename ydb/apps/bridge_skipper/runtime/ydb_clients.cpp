#include "ydb_clients.h"

#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/config/config.h>
#include <yaml-cpp/yaml.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/cast.h>

#include <algorithm>
#include <utility>

namespace NBridgeSkipper {
namespace {

std::string Std(TStringBuf text) {
    return std::string(text.data(), text.size());
}

void FillStatus(TRpcResult& rpc, const NYdb::TStatus& status, const TRedactor& redactor) {
    rpc.Success = status.IsSuccess();
    rpc.SdkStatus = status.GetStatus();
    rpc.Status = SdkStatusName(status.GetStatus());
    rpc.Issues = redactor.Issues(status);
}

void Fail(TStateResult& result, TStringBuf status) {
    result.Rpc.Success = false;
    result.Rpc.Status = status;
    result.View.reset();
    result.Error = status;
    result.Connectivity.reset();
}

void Fail(TSelfCheckResult& result, TStringBuf status) {
    result.Rpc.Success = false;
    result.Rpc.Status = status;
    result.Reply.reset();
    result.ResponderNodeId.reset();
}

void Fail(TUpdateResult& result, TStringBuf status) {
    result.Rpc.Success = false;
    result.Rpc.Status = status;
    result.Outcome = EMutationOutcome::Ambiguous;
}

void Fail(TInventoryResult& result, TStringBuf status) {
    result.Rpc.Success = false;
    result.Rpc.Status = status;
    result.Endpoints.clear();
    result.Error = status;
}

struct TTokenFactory final : NYdb::ICredentialsProviderFactory {
    std::shared_ptr<TTokenSource> Source;

    explicit TTokenFactory(std::shared_ptr<TTokenSource> source)
        : Source(std::move(source)) {
    }

    NYdb::TCredentialsProviderPtr CreateProvider() const override {
        return Source;
    }
};

NYdb::TDriverConfig MakeDriverConfig(
    const TConfig& config,
    const TLoadedCredentials& credentials,
    const std::shared_ptr<TTokenSource>& tokens)
{
    const auto& endpoint = config.Endpoints.begin()->second.front();
    NYdb::TDriverConfig driver(Std(endpoint.Uri));
    driver.SetDatabase("").SetDiscoveryMode(NYdb::EDiscoveryMode::Off);
    if (endpoint.Scheme == "grpcs") {
        driver.UseSecureConnection(credentials.Ca ? Std(*credentials.Ca) : std::string());
    }
    if (credentials.ClientCert && credentials.ClientKey) {
        driver.UseClientCertificate(Std(*credentials.ClientCert), Std(*credentials.ClientKey));
    }
    if (tokens->IsValid()) {
        driver.SetCredentialsProviderFactory(std::make_shared<TTokenFactory>(tokens));
    }
    return driver;
}

class TYdbClients final : public IYdbClients {
public:
    TYdbClients(const TConfig& config, const TLoadedCredentials& credentials, TYdbClock clock, TTokenLoader tokenLoader)
        : Timing(config.Timing)
        , Tokens(std::make_shared<TTokenSource>(config.Auth.TokenFile, credentials.Token, std::move(tokenLoader)))
        , Redactor(std::make_shared<const TRedactor>(Tokens))
        , Clock(std::move(clock))
        , Driver(std::make_shared<NYdb::TDriver>(MakeDriverConfig(config, credentials, Tokens))) {
        // Construct clients before actor registration; subsequent RPC starts do
        // not perform client discovery or mutate this endpoint inventory.
        for (const auto& [pile, endpoints] : config.Endpoints) {
            for (const auto& endpoint : endpoints) {
                Clients.emplace(endpoint.Uri, std::make_unique<TClients>(*Driver, endpoint, pile));
            }
        }
    }

    void GetState(TRpcRequest request, EYdbMethod method, std::function<void(TStateResult)> completed) override {
        TStateResult base;
        base.Rpc.Request = std::move(request);
        base.Rpc.Method = method;
        auto* client = Find(base.Rpc.Request);
        if (!client || (method != EYdbMethod::PlainState && method != EYdbMethod::ConnectivityState)) {
            CompleteInvalid(std::move(base), std::move(completed));
            return;
        }
        const auto settings = NYdb::NBridge::TGetClusterStateSettings()
            .ClientTimeout(Timing.ReadTimeout).OperationTimeout(Timing.ReadTimeout)
            .IncludePileConnectivity(method == EYdbMethod::ConnectivityState);
        Launch(std::move(base), std::move(completed), [client, settings] {
            return client->Bridge.GetClusterState(settings);
        }, DecodeStateResult);
    }

    void SelfCheck(TRpcRequest request, std::function<void(TSelfCheckResult)> completed) override {
        TSelfCheckResult base;
        base.Rpc.Request = std::move(request);
        base.Rpc.Method = EYdbMethod::SelfCheck;
        auto* client = Find(base.Rpc.Request);
        if (!client) {
            CompleteInvalid(std::move(base), std::move(completed));
            return;
        }
        const auto settings = NYdb::NMonitoring::TSelfCheckSettings()
            .ReturnVerboseStatus(false).NoMerge(true).NoCache(true)
            .OperationTimeout(Timing.SelfCheckOperationTimeout).ClientTimeout(Timing.SelfCheckClientTimeout);
        Launch(std::move(base), std::move(completed), [client, settings] {
            return client->Monitoring.SelfCheck(settings);
        }, DecodeSelfCheckResult);
    }

    bool Update(TRpcRequest request, TPlan plan, TUpdateAuthorization authorization,
        std::function<void(TUpdateResult)> completed) override
    {
        TUpdateResult base;
        base.Rpc.Request = std::move(request);
        base.Rpc.Method = EYdbMethod::Update;
        base.Plan = std::move(plan);
        auto* client = Find(base.Rpc.Request);
        if (!client || base.Plan.Endpoint != base.Rpc.Request.Endpoint) {
            CompleteInvalid(std::move(base), std::move(completed));
            return true;
        }
        std::vector<NYdb::NBridge::TPileStateUpdate> updates;
        for (const auto& [pile, state] : base.Plan.Update) {
            updates.push_back({Std(pile), static_cast<NYdb::NBridge::EPileState>(state), 0});
        }
        const auto settings = NYdb::NBridge::TUpdateClusterStateSettings()
            .ExpectedGeneration(base.Plan.ExpectedGeneration)
            .OperationTimeout(Timing.MutationOperationTimeout).ClientTimeout(Timing.MutationClientTimeout);
        const auto exactPlan = base.Plan;
        Tokens->Reload();
        return Launch(std::move(base), std::move(completed), [client, updates = std::move(updates), settings] {
            return client->Bridge.UpdateClusterState(updates, {}, settings);
        }, [exactPlan](const NYdb::TStatus& result, TRpcResult rpc, const TRedactor& redactor) {
            return DecodeUpdateResult(result, std::move(rpc), exactPlan, redactor);
        }, authorization.ValidUntil);
    }

    void AddEndpoint(const TEndpoint& endpoint, const TString& pile) override {
        Y_ENSURE(!Clients.contains(endpoint.Uri));
        Clients.emplace(endpoint.Uri, std::make_unique<TClients>(*Driver, endpoint, pile));
    }

    void RemoveEndpoint(const TString& endpoint) override {
        Clients.erase(endpoint);
    }

    void Discover(TRpcRequest request, std::function<void(TInventoryResult)> completed) override {
        TInventoryResult base;
        base.Rpc.Request = std::move(request);
        base.Rpc.Method = EYdbMethod::Inventory;
        auto* client = Find(base.Rpc.Request);
        if (!client) {
            CompleteInvalid(std::move(base), std::move(completed));
            return;
        }
        const auto timeout = Timing.ReadTimeout + TDuration::Seconds(2);
        // The chain owns its driver and completion; it never touches the actor
        // or mutates the SDK endpoint map from an SDK callback.
        base.Rpc.SentAt = Clock();
        auto finish = [completed = std::move(completed), clock = Clock, tokens = Tokens](TInventoryResult result) mutable {
            if (result.Rpc.SdkStatus == NYdb::EStatus::UNAUTHORIZED) {
                tokens->Reload();
            }
            result.Rpc.FinishedAt = clock();
            completed(std::move(result));
        };
        const auto settings = NYdb::NConfig::TFetchAllConfigsSettings()
            .OperationTimeout(Timing.ReadTimeout).ClientTimeout(timeout);
        try {
            auto future = client->Config.FetchAllConfigs(settings);
            future.Subscribe([base = std::move(base), finish, driver = Driver, redactor = Redactor,
                    timeout](const auto& settled) mutable {
                try {
                    const auto& config = settled.GetValueSync();
                    FillStatus(base.Rpc, config, *redactor);
                    if (!base.Rpc.Success) {
                        finish(std::move(base));
                        return;
                    }
                    TString mainYaml;
                    TString storageYaml;
                    if (!config.Transient()) {
                        for (const auto& entry : config.GetConfigs()) {
                            if (std::holds_alternative<NYdb::NConfig::TMainConfigIdentity>(entry.Identity)) {
                                Y_ENSURE(mainYaml.empty(), "ambiguous main config");
                                mainYaml = entry.Config;
                            } else if (std::holds_alternative<NYdb::NConfig::TStorageConfigIdentity>(entry.Identity)) {
                                Y_ENSURE(storageYaml.empty(), "ambiguous storage config");
                                storageYaml = entry.Config;
                            }
                        }
                    }
                    if (mainYaml.empty()) {
                        base.Error = "DISCOVERY_CONFIG_UNAVAILABLE";
                        finish(std::move(base));
                        return;
                    }
                    const auto database = DiscoveryDatabase(mainYaml, storageYaml);
                    const auto endpoint = ParseEndpoint(base.Rpc.Request.Endpoint);
                    auto discovery = std::make_shared<NYdb::NDiscovery::TDiscoveryClient>(*driver,
                        MakePinnedClientSettings(endpoint).Database(Std(database)));
                    auto listed = discovery->ListEndpoints(NYdb::NDiscovery::TListEndpointsSettings().ClientTimeout(timeout));
                    listed.Subscribe([base = std::move(base), discovery, finish, redactor,
                            scheme = endpoint.Scheme](const auto& settled) mutable {
                        try {
                            finish(DecodeInventoryResult(settled.GetValueSync(), std::move(base.Rpc), *redactor, scheme));
                        } catch (...) {
                            Fail(base, "SDK_RESULT_ERROR");
                            finish(std::move(base));
                        }
                    });
                } catch (...) {
                    base.Error = "INVALID_DISCOVERY_CONFIG";
                    finish(std::move(base));
                }
            });
        } catch (...) {
            Fail(base, "SDK_START_ERROR");
            finish(std::move(base));
        }
    }

    void Stop() override {
        Driver->Stop(true);
    }

private:
    struct TClients {
        TString Pile;
        NYdb::NBridge::TBridgeClient Bridge;
        NYdb::NMonitoring::TMonitoringClient Monitoring;
        NYdb::NConfig::TConfigClient Config;

        TClients(const NYdb::TDriver& driver, const TEndpoint& endpoint, const TString& pile)
            : Pile(pile)
            , Bridge(driver, MakePinnedClientSettings(endpoint))
            , Monitoring(driver, MakePinnedClientSettings(endpoint))
            , Config(driver, MakePinnedClientSettings(endpoint)) {
        }
    };

    TClients* Find(const TRpcRequest& request) {
        const auto it = Clients.find(request.Endpoint);
        return it != Clients.end() && it->second->Pile == request.Pile ? it->second.get() : nullptr;
    }

    template<class TReply>
    void CompleteInvalid(TReply reply, std::function<void(TReply)> completed) {
        Fail(reply, "INVALID_RPC_REQUEST");
        reply.Rpc.FinishedAt = Clock();
        completed(std::move(reply));
    }

    template<class TReply, class TLaunch, class TDecode>
    bool Launch(TReply base, std::function<void(TReply)> completed, TLaunch launch, TDecode decode,
        TMonotonic validUntil = TMonotonic::Max())
    {
        decltype(launch()) future;
        try {
            base.Rpc.SentAt = Clock();
            if (base.Rpc.SentAt >= validUntil) {
                return false;
            }
            future = launch();
        } catch (...) {
            // Lower-level exceptions can contain remote text or credentials.
            Fail(base, "SDK_START_ERROR");
            base.Rpc.FinishedAt = Clock();
            completed(std::move(base));
            return true;
        }
        // No actor pointer, activation context, driver or client reference is
        // captured. The coordinator keeps ActorSystem alive through Stop(true).
        future.Subscribe([base = std::move(base), completed = std::move(completed),
                decode = std::move(decode), redactor = Redactor, tokens = Tokens, clock = Clock](const auto& settled) mutable {
            try {
                base = decode(settled.GetValueSync(), base.Rpc, *redactor);
            } catch (...) {
                Fail(base, "SDK_RESULT_ERROR");
            }
            if (base.Rpc.SdkStatus == NYdb::EStatus::UNAUTHORIZED) {
                tokens->Reload();
            }
            base.Rpc.FinishedAt = clock();
            completed(std::move(base));
        });
        return true;
    }

    TTimingConfig Timing;
    std::shared_ptr<TTokenSource> Tokens;
    std::shared_ptr<const TRedactor> Redactor;
    TYdbClock Clock;
    std::shared_ptr<NYdb::TDriver> Driver;
    std::map<TString, std::unique_ptr<TClients>> Clients;
};

} // namespace

TTokenSource::TTokenSource(const TMaybe<TString>& file, const TMaybe<TString>& token, TTokenLoader loader)
    : File(file)
    , Current(token.GetOrElse(TString()))
    , Loader(std::move(loader)) {
}

TString TTokenSource::Token() const {
    std::lock_guard lock(Mutex);
    return Current;
}

void TTokenSource::Reload() {
    if (!File) {
        return;
    }
    TMaybe<TString> token;
    try {
        token = Loader(File);
    } catch (const yexception&) {
        return;
    }
    {
        std::lock_guard lock(Mutex);
        if (Current == *token) {
            return;
        }
        Current = *token;
    }
    Cerr << "{\"event\":\"token_reloaded\",\"level\":\"info\",\"category\":\"credentials\"}\n";
}

std::string TTokenSource::GetAuthInfo() const {
    return Std(Token());
}

bool TTokenSource::IsValid() const {
    return !Token().empty();
}

TRedactor::TRedactor(std::shared_ptr<const TTokenSource> tokens)
    : Tokens(std::move(tokens)) {
}

TRedactor::TRedactor(const TLoadedCredentials& credentials)
    : TRedactor(std::make_shared<const TTokenSource>(Nothing(), credentials.Token)) {
}

TString TRedactor::Redact(TStringBuf text, size_t limit) const {
    const TString token = Tokens->Token();
    TString result(text);
    size_t offset = 0;
    while (!token.empty() && (offset = result.find(token, offset)) != TString::npos) {
        result.replace(offset, token.size(), "[REDACTED]");
        offset += 10;
    }
    // Redact the complete input before truncating it across a secret boundary.
    result.resize(std::min(result.size(), limit));
    return result;
}

TString TRedactor::Issues(const NYdb::TStatus& status) const {
    return Redact(status.GetIssues().ToString());
}

NYdb::TCommonClientSettings MakePinnedClientSettings(const TEndpoint& endpoint) {
    const auto parsed = ParseEndpoint(endpoint.Uri);
    return NYdb::TCommonClientSettings().Database("")
        .DiscoveryEndpoint(Std(parsed.Host + ":" + ::ToString(parsed.Port)))
        .DiscoveryMode(NYdb::EDiscoveryMode::Off);
}

TString DiscoveryDatabase(TStringBuf yaml, TStringBuf storageYaml) {
    try {
        const auto parse = [](TStringBuf text) {
            Y_ENSURE(text.size() <= 4 * 1024 * 1024);
            const auto docs = YAML::LoadAll(std::string(text.data(), text.size()));
            Y_ENSURE(docs.size() == 1);
            const auto config = docs.front()["config"];
            Y_ENSURE(config.IsMap());
            return config;
        };
        const auto main = parse(yaml);
        const auto storage = storageYaml.empty() ? YAML::Node(YAML::NodeType::Map) : parse(storageYaml);
        // Node startup fills missing top-level keys from StorageConfig into
        // MainConfig. Existing maps are kept whole, without recursive merging.
        const auto field = [&](const char* key) {
            const auto value = main[key];
            return value.IsDefined() ? value : storage[key];
        };
        // Match YDB's raw YAML domain-name precedence. The v1 downgrade
        // transform rejects dedicated storage and independently configured boards.
        // TEphemeralInputFields.DomainName defaults to Root on the server.
        TString value = "Root";
        const auto modernName = field("domain_name");
        if (modernName.IsDefined()) {
            Y_ENSURE(modernName.IsScalar());
            value = modernName.Scalar();
        }
        const auto pools = field("storage_pool_types");
        Y_ENSURE(!pools.IsDefined() || pools.IsSequence());
        // MoveFields rebuilds the domain only for a nonempty top-level pool
        // list. Otherwise PrepareDomainsConfig keeps an existing domain name.
        if (!modernName.IsDefined() || !pools.IsDefined() || pools.size() == 0) {
            const auto domainsConfig = field("domains_config");
            if (domainsConfig.IsDefined()) {
                Y_ENSURE(domainsConfig.IsMap());
                const auto domains = domainsConfig["domain"];
                if (domains.IsDefined()) {
                    Y_ENSURE(domains.IsSequence() && domains.size() <= 1);
                    if (domains.size() == 1) {
                        const auto name = domains[0]["name"];
                        Y_ENSURE(name.IsScalar());
                        value = name.Scalar();
                    }
                }
            }
        }
        Y_ENSURE(!value.empty() && value.find_first_of("/\\\r\n") == TString::npos && value.find('\0') == TString::npos);
        return "/" + value;
    } catch (...) {
        throw yexception() << "INVALID_DISCOVERY_CONFIG";
    }
}

TInventoryResult DecodeInventoryResult(const NYdb::NDiscovery::TListEndpointsResult& result,
    TRpcResult rpc, const TRedactor& redactor, TStringBuf scheme)
{
    TInventoryResult decoded;
    decoded.Rpc = std::move(rpc);
    FillStatus(decoded.Rpc, result, redactor);
    if (!decoded.Rpc.Success) {
        return decoded;
    }
    if (result.GetEndpointsInfo().size() > 16384) {
        decoded.Error = "DISCOVERY_INVENTORY_TOO_LARGE";
        return decoded;
    }
    std::map<TString, std::pair<TString, ui32>> seen;
    std::map<ui32, TString> piles;
    for (const auto& item : result.GetEndpointsInfo()) {
        // Preserve the seed transport and its TLS verification. Endpoint-specific
        // certificate-name overrides are not supported by pinned SDK clients.
        if (item.Ssl != (scheme == "grpcs") || !item.SslTargetNameOverride.empty()) {
            continue;
        }
        try {
            Y_ENSURE(item.NodeId != 0 && item.BridgePileName.find('\0') == std::string::npos);
            const std::pair<TString, ui32> identity{TString(item.BridgePileName), item.NodeId};
            const auto [node, newNode] = piles.emplace(item.NodeId, identity.first);
            Y_ENSURE(newNode || node->second == identity.first);
            const auto addAddress = [&](TStringBuf address, bool optionalAlias = false) {
                TEndpoint endpoint;
                try {
                    TString host(address);
                    if (host.find(':') != TString::npos && !host.StartsWith('[')) {
                        host = "[" + host + "]";
                    }
                    endpoint = ParseEndpoint(TString(scheme) + "://" + host + ":" + ::ToString(item.Port));
                } catch (const yexception&) {
                    // Unsupported optional locators must not discard usable
                    // addresses. Identity conflicts below remain fatal.
                    if (optionalAlias) {
                        return;
                    }
                    throw;
                }
                const auto [it, inserted] = seen.emplace(endpoint.Uri, identity);
                Y_ENSURE(inserted || it->second == identity);
                if (inserted) {
                    if (decoded.Endpoints.size() == 16384) {
                        decoded.Error = "DISCOVERY_INVENTORY_TOO_LARGE";
                        return;
                    }
                    decoded.Endpoints.push_back({std::move(endpoint), identity.first, identity.second});
                }
            };
            addAddress(item.Address);
            // TLS IP connections need certificate-name overrides, unsupported
            // by pinned clients. Plaintext clients can use the published IPs.
            if (scheme == "grpc") {
                for (const auto& addresses : {&item.IPv4Addrs, &item.IPv6Addrs}) {
                    for (const auto& address : *addresses) {
                        if (!decoded.Error.empty()) {
                            break;
                        }
                        addAddress(address, /*optionalAlias=*/true);
                    }
                }
            }
            if (!decoded.Error.empty()) {
                decoded.Endpoints.clear();
                return decoded;
            }
        } catch (...) {
            decoded.Endpoints.clear();
            decoded.Error = "INVALID_DISCOVERY_ENDPOINT";
            return decoded;
        }
    }
    return decoded;
}

TString ToString(EYdbMethod method) {
    switch (method) {
        case EYdbMethod::PlainState:
            return "get_cluster_state";
        case EYdbMethod::ConnectivityState:
            return "get_cluster_state_connectivity";
        case EYdbMethod::SelfCheck:
            return "self_check";
        case EYdbMethod::Inventory:
            return "endpoint_inventory";
        case EYdbMethod::Update:
            return "update_cluster_state";
    }
    return "OTHER";
}

TString SdkStatusName(NYdb::EStatus status) {
    try {
        return ::ToString(status);
    } catch (const yexception&) {
        // The generated serializer throws for a value it does not know.
        return "OTHER";
    }
}

EMutationOutcome ClassifyUpdateOutcome(NYdb::EStatus status) {
    // Operation-level UNAVAILABLE and PRECONDITION_FAILED are returned only
    // before the update reaches distconf; transport errors stay Ambiguous.
    switch (status) {
        case NYdb::EStatus::SUCCESS:
            return EMutationOutcome::Success;
        case NYdb::EStatus::ABORTED:
            return EMutationOutcome::GenerationMoved;
        case NYdb::EStatus::UNAVAILABLE:
        case NYdb::EStatus::PRECONDITION_FAILED:
            return EMutationOutcome::NotAppliedHere;
        case NYdb::EStatus::BAD_REQUEST:
        case NYdb::EStatus::UNAUTHORIZED:
            return EMutationOutcome::Rejected;
        default:
            return EMutationOutcome::Ambiguous;
    }
}

TStateResult DecodeStateResult(
    const NYdb::NBridge::TGetClusterStateResult& result,
    TRpcResult rpc,
    const TRedactor& redactor)
{
    TStateResult decoded;
    decoded.Rpc = std::move(rpc);
    FillStatus(decoded.Rpc, result, redactor);
    if (!decoded.Rpc.Success) {
        decoded.Error = decoded.Rpc.Status;
        return decoded;
    }
    TClusterView view;
    view.Endpoint = decoded.Rpc.Request.Endpoint;
    view.SentAt = decoded.Rpc.SentAt;
    view.Generation = result.GetGeneration();
    view.ConversionInProgress = result.IsConversionInProgress();
    // Detect wire-vector duplicates before inserting anything in the map.
    std::set<TString> names;
    for (const auto& pile : result.GetState()) {
        if (!names.insert(TString(pile.PileName)).second) {
            decoded.Error = "duplicate pile name in GetClusterState";
            return decoded;
        }
        if (!IsKnownState(static_cast<EPileState>(pile.State))) {
            decoded.Error = "unknown pile state in GetClusterState";
            return decoded;
        }
    }
    for (const auto& pile : result.GetState()) {
        view.Piles.emplace(TString(pile.PileName), static_cast<EPileState>(pile.State));
    }
    if (const auto& pending = result.GetPendingPileOperation()) {
        const auto kind = pending->Kind == NYdb::NBridge::TPendingPileOperation::EKind::ADD ? "ADD"
            : pending->Kind == NYdb::NBridge::TPendingPileOperation::EKind::REMOVE ? "REMOVE" : "UNKNOWN";
        view.Pending = TPendingOperation{kind, TString(pending->PileName), redactor.Redact(pending->ErrorReason)};
    }
    decoded.Error = ValidateClusterView(view);
    if (!decoded.Error.empty()) {
        return decoded;
    }
    if (const auto& report = result.GetPileConnectivity()) {
        names.clear();
        for (const auto& pile : report->Piles) {
            if (!names.insert(TString(pile.PileName)).second) {
                decoded.Error = "duplicate pile name in connectivity report";
                return decoded;
            }
        }
        TConnectivity connectivity;
        connectivity.VantagePile = decoded.Rpc.Request.Pile;
        connectivity.ReporterNodeId = report->ReporterNodeId;
        connectivity.ReporterIsRoot = report->ReporterIsRoot;
        connectivity.BindingInProgress = report->BindingInProgress;
        connectivity.Generation = report->Generation;
        connectivity.SentAt = decoded.Rpc.SentAt;
        for (const auto& pile : report->Piles) {
            connectivity.Piles.emplace(TString(pile.PileName), TPileConnectivity{
                pile.StaticNodes, pile.ConnectedStaticNodes, pile.HasNodeQuorum, pile.InConfigQuorum, pile.InDataQuorum});
        }
        decoded.Connectivity = std::move(connectivity);
    }
    decoded.View = std::move(view);
    return decoded;
}

TSelfCheckResult DecodeSelfCheckResult(
    const NYdb::NMonitoring::TSelfCheckResult& result,
    TRpcResult rpc,
    const TRedactor& redactor)
{
    TSelfCheckResult decoded;
    decoded.Rpc = std::move(rpc);
    FillStatus(decoded.Rpc, result, redactor);
    if (!decoded.Rpc.Success) {
        return decoded;
    }
    // Runs once per reply on the callback thread, so the actor never touches
    // a reply of up to 50 MiB.
    const auto& proto = NYdb::TProtoAccessor::GetProto(result);
    TSelfCheckReply reply;
    reply.Endpoint = decoded.Rpc.Request.Endpoint;
    reply.SentAt = decoded.Rpc.SentAt;
    if (proto.has_location()) {
        decoded.ResponderNodeId = proto.location().id();
        if (proto.location().has_pile()) {
            reply.ResponderPile = TString(proto.location().pile().name());
        }
    }
    reply.Classification = ClassifySelfCheck(proto);
    for (auto& note : reply.Classification.Notes) {
        note = redactor.Redact(note);
    }
    decoded.Reply = std::move(reply);
    return decoded;
}

TUpdateResult DecodeUpdateResult(
    const NYdb::TStatus& result,
    TRpcResult rpc,
    TPlan plan,
    const TRedactor& redactor)
{
    TUpdateResult decoded;
    decoded.Rpc = std::move(rpc);
    decoded.Plan = std::move(plan);
    FillStatus(decoded.Rpc, result, redactor);
    decoded.Outcome = ClassifyUpdateOutcome(result.GetStatus());
    return decoded;
}

std::shared_ptr<IYdbClients> CreateYdbClients(
    const TConfig& config,
    const TLoadedCredentials& credentials,
    TYdbClock clock,
    TTokenLoader tokenLoader)
{
    return std::make_shared<TYdbClients>(config, credentials, std::move(clock), std::move(tokenLoader));
}

} // namespace NBridgeSkipper
