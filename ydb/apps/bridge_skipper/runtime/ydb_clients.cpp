#include "ydb_clients.h"

#include <ydb/public/api/protos/ydb_monitoring.pb.h>
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
        , Driver(MakeDriverConfig(config, credentials, Tokens)) {
        // Construct clients before actor registration; subsequent RPC starts do
        // not perform client discovery or mutate this endpoint inventory.
        for (const auto& [pile, endpoints] : config.Endpoints) {
            for (const auto& endpoint : endpoints) {
                Clients.emplace(endpoint.Uri, std::make_unique<TClients>(Driver, endpoint, pile));
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

    void Stop() override {
        Driver.Stop(true);
    }

private:
    struct TClients {
        TString Pile;
        NYdb::NBridge::TBridgeClient Bridge;
        NYdb::NMonitoring::TMonitoringClient Monitoring;

        TClients(const NYdb::TDriver& driver, const TEndpoint& endpoint, const TString& pile)
            : Pile(pile)
            , Bridge(driver, MakePinnedClientSettings(endpoint))
            , Monitoring(driver, MakePinnedClientSettings(endpoint)) {
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
            if (base.Rpc.SentAt > validUntil) {
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
    NYdb::TDriver Driver;
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

TString TRedactor::Redact(TStringBuf text) const {
    const TString token = Tokens->Token();
    TString result(text);
    size_t offset = 0;
    while (!token.empty() && (offset = result.find(token, offset)) != TString::npos) {
        result.replace(offset, token.size(), "[REDACTED]");
        offset += 10;
    }
    // Redact the complete input before truncating it across a secret boundary.
    result.resize(std::min<size_t>(result.size(), 4096));
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

TString ToString(EYdbMethod method) {
    switch (method) {
        case EYdbMethod::PlainState:
            return "get_cluster_state";
        case EYdbMethod::ConnectivityState:
            return "get_cluster_state_connectivity";
        case EYdbMethod::SelfCheck:
            return "self_check";
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
