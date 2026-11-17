#pragma once

#include <ydb/apps/bridge_skipper/config/config.h>
#include <ydb/apps/bridge_skipper/domain/health.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/common_client/settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/monitoring/monitoring.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/time_provider/monotonic.h>

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace NBridgeSkipper {

// The two GetClusterState variants have independent outstanding-call slots.
enum class EYdbMethod { PlainState, ConnectivityState, SelfCheck, Update, Inventory };

// The metric and log name of a method.
TString ToString(EYdbMethod method);

struct TRpcRequest {
    TString Endpoint;
    TString Pile;
    ui64 Sequence = 0;
};

struct TRpcResult {
    TRpcRequest Request;
    EYdbMethod Method = EYdbMethod::PlainState;
    TMonotonic SentAt;
    TMonotonic FinishedAt;
    bool Success = false;
    // Absent for local failures (SDK_START_ERROR, SDK_RESULT_ERROR, INVALID_RPC_REQUEST).
    std::optional<NYdb::EStatus> SdkStatus;
    TString Status;
    TString Issues;
};

struct TStateResult {
    TRpcResult Rpc;
    // Decode is the only validator: View is present only when it is valid,
    // otherwise Error explains why.
    std::optional<TClusterView> View;
    TString Error;
    std::optional<TConnectivity> Connectivity;
};

struct TSelfCheckResult {
    TRpcResult Rpc;
    // Classified and redacted once at decode; the raw reply is not kept.
    std::optional<TSelfCheckReply> Reply;
    std::optional<ui32> ResponderNodeId;
};

struct TDiscoveredEndpoint {
    TEndpoint Endpoint;
    TString Pile;
    ui32 NodeId = 0;
};

struct TInventoryResult {
    TRpcResult Rpc;
    std::vector<TDiscoveredEndpoint> Endpoints;
    TString Error;
};

class TRedactor;

// Raw MainConfig/StorageConfig supply the discovery database with main-first merging;
// no conventional root name is assumed. Throws without copying server YAML.
TString DiscoveryDatabase(TStringBuf yaml, TStringBuf storageYaml = {});
TInventoryResult DecodeInventoryResult(const NYdb::NDiscovery::TListEndpointsResult& result,
    TRpcResult rpc, const TRedactor& redactor, TStringBuf scheme);

struct TUpdateResult {
    TRpcResult Rpc;
    TPlan Plan;
    EMutationOutcome Outcome = EMutationOutcome::Ambiguous;
};

// Ephemeral launch permission; never persisted with the same-generation overlay.
struct TUpdateAuthorization {
    // At this instant the supporting evidence is already expired.
    TMonotonic ValidUntil;
};

using TTokenLoader = std::function<TMaybe<TString>(const TMaybe<TString>&)>;

// The current token. The SDK asks for it on every request; the file is
// re-read before every UpdateClusterState and after any UNAUTHORIZED reply
// (B8), so a rotated token takes effect without a restart.
class TTokenSource final : public NYdb::ICredentialsProvider {
public:
    TTokenSource(const TMaybe<TString>& file, const TMaybe<TString>& token, TTokenLoader loader = LoadToken);
    TString Token() const;
    // Keeps the current token when the read fails; logs token_reloaded once per change.
    void Reload();
    std::string GetAuthInfo() const override;
    bool IsValid() const override;

private:
    TMaybe<TString> File;
    mutable std::mutex Mutex;
    TString Current;
    TTokenLoader Loader;
};

// Replaces the token, the only secret that could plausibly appear in remote
// issue text, and bounds the result.
class TRedactor {
public:
    explicit TRedactor(std::shared_ptr<const TTokenSource> tokens);
    // A fixed token, for callers without a token file.
    explicit TRedactor(const TLoadedCredentials& credentials);
    TString Redact(TStringBuf text, size_t limit = 4096) const;
    TString Issues(const NYdb::TStatus& status) const;

private:
    std::shared_ptr<const TTokenSource> Tokens;
};

NYdb::TCommonClientSettings MakePinnedClientSettings(const TEndpoint& endpoint);
// The SDK's own enum name; OTHER for a value the SDK does not know.
TString SdkStatusName(NYdb::EStatus status);
// The A5 table; any other status, including an unknown value, is Ambiguous.
EMutationOutcome ClassifyUpdateOutcome(NYdb::EStatus status);

// Decode before constructing name-keyed maps; an invalid response retains Error.
TStateResult DecodeStateResult(
    const NYdb::NBridge::TGetClusterStateResult& result,
    TRpcResult rpc,
    const TRedactor& redactor);

TSelfCheckResult DecodeSelfCheckResult(
    const NYdb::NMonitoring::TSelfCheckResult& result,
    TRpcResult rpc,
    const TRedactor& redactor);

TUpdateResult DecodeUpdateResult(
    const NYdb::TStatus& result,
    TRpcResult rpc,
    TPlan plan,
    const TRedactor& redactor);

class IYdbClients {
public:
    virtual ~IYdbClients() = default;

    virtual void GetState(TRpcRequest request, EYdbMethod method, std::function<void(TStateResult)> completed) = 0;
    virtual void SelfCheck(TRpcRequest request, std::function<void(TSelfCheckResult)> completed) = 0;
    // False: local authorization expired, no SDK invocation and no completion.
    // True: an attempt was accepted and completed owns its eventual result.
    virtual bool Update(TRpcRequest request, TPlan plan, TUpdateAuthorization authorization,
        std::function<void(TUpdateResult)> completed) = 0;
    virtual void Discover(TRpcRequest request, std::function<void(TInventoryResult)> completed) = 0;
    // Owning actor only; removal requires all endpoint invocations to settle.
    virtual void AddEndpoint(const TEndpoint& endpoint, const TString& pile) = 0;
    virtual void RemoveEndpoint(const TString& endpoint) = 0;
    // Coordinator only. The actor system must outlive this drain and its callbacks.
    virtual void Stop() = 0;
};

using TYdbClock = std::function<TMonotonic()>;

// Call methods from the owning actor. Completions may run on foreign SDK threads;
// they own their payload and must marshal it to the actor without accessing it.
// Completion functions must not throw.
std::shared_ptr<IYdbClients> CreateYdbClients(
    const TConfig& config,
    const TLoadedCredentials& credentials,
    TYdbClock clock = [] {
        return TMonotonic::Now();
    },
    TTokenLoader tokenLoader = LoadToken);

} // namespace NBridgeSkipper
