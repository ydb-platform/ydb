#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <map>

namespace NBridgeSkipper {

struct TEndpoint {
    TString Uri;
    TString Scheme;
    TString Host; // IPv6 brackets are retained, as in Uri.
    ui16 Port = 0;
};

TEndpoint ParseEndpoint(TStringBuf uri);

// An HTTP Host authority. Names and IP literals are normalized without DNS;
// an omitted port remains distinct when matching configured authorities.
struct THttpAuthority {
    TString Host;
    TMaybe<ui16> Port;
    bool Numeric = false;

    bool operator==(const THttpAuthority&) const = default;
};

// Throws yexception for malformed host[:port], userinfo, paths or wildcards.
THttpAuthority ParseHttpAuthority(TStringBuf authority);

struct TYdbAuthConfig {
    TMaybe<TString> CaFile;
    TMaybe<TString> ClientCertFile;
    TMaybe<TString> ClientKeyFile;
    TMaybe<TString> TokenFile;
};

struct THttpConfig {
    TString Listen = "127.0.0.1:8080";
    // Additional exact Host authorities (host or host:port); no DNS lookups.
    TVector<TString> TrustedAuthorities;
};

// Keep stale positive evidence and failure confirmation inside the normal
// 20-second failover budget. Safety blockers and post-role-change BSC warm-up
// can intentionally take longer; no wall-clock deadline authorizes a mutation.
struct TTimingConfig {
    TDuration StatePollInterval = TDuration::Seconds(1);
    TDuration ReadTimeout = TDuration::Seconds(1);
    TDuration RoundInterval = TDuration::Seconds(1);
    TDuration SelfCheckOperationTimeout = TDuration::Seconds(4);
    // Not configurable: the SelfCheck and update client deadlines are their
    // operation timeouts + 2 s. GetClusterState uses ReadTimeout for both.
    TDuration SelfCheckClientTimeout = TDuration::Seconds(6);
    TDuration EvidenceTtl = TDuration::Seconds(8);
    TDuration Confirm = TDuration::Seconds(2);
    TDuration BscWarmup = TDuration::Seconds(30);
    TDuration MutationOperationTimeout = TDuration::Seconds(25);
    TDuration MutationClientTimeout = TDuration::Seconds(27);
};

struct TLoggingConfig {
    TString Level = "info";
};

struct TConfig {
    TString ClusterId;
    // Empty pile names are valid and are verified with SelfCheck like any other.
    std::map<TString, TVector<TEndpoint>> Endpoints;
    TYdbAuthConfig Auth;
    THttpConfig Http;
    TTimingConfig Timing;
    ui32 HealthEndpointsPerPile = 2;
    TLoggingConfig Logging;
};

// These functions throw yexception on invalid input. Parsing validates all fields.
TConfig ParseConfig(TStringBuf yaml);
TConfig LoadConfig(const TString& path);

// Secrets as loaded at startup (the token is re-read later by TTokenSource):
// never include this object in state or diagnostics.
struct TLoadedCredentials {
    TMaybe<TString> Ca;
    TMaybe<TString> ClientCert;
    TMaybe<TString> ClientKey;
    TMaybe<TString> Token;
};

TLoadedCredentials LoadCredentials(const TConfig& config);
// The token part of LoadCredentials: Nothing() without a file.
TMaybe<TString> LoadToken(const TMaybe<TString>& file);

enum class EEntrypointAction { Run, CheckConfig, Version, Help };

struct TEntrypointArguments {
    EEntrypointAction Action = EEntrypointAction::Run;
    TString ConfigFile;
    TString StateDir;
    bool ClearPendingMutation = false;
};

// Arguments exclude argv[0]. Help/version need no runtime configuration;
// only Run requires a state directory.
TEntrypointArguments ParseEntrypointArguments(const TVector<TString>& arguments);
TStringBuf EntrypointHelp();

} // namespace NBridgeSkipper
