#include "config.h"

#include <yaml-cpp/yaml.h>

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

#include <initializer_list>
#include <limits>

namespace NBridgeSkipper {
namespace {

constexpr size_t MaxConfigBytes = 1024 * 1024;
constexpr size_t MaxCredentialBytes = 16 * 1024 * 1024;
constexpr TDuration ClientDeadlineMargin = TDuration::Seconds(2);

void Require(bool condition, TStringBuf message) {
    if (!condition) {
        throw yexception() << message;
    }
}

TString Scalar(const YAML::Node& node, TStringBuf path) {
    Require(node.IsScalar(), TStringBuilder() << path << " must be a scalar");
    return TString(node.Scalar());
}

void Map(const YAML::Node& node, TStringBuf path, std::initializer_list<TStringBuf> allowed) {
    Require(node.IsMap(), TStringBuilder() << path << " must be a mapping");
    THashSet<TString> seen;
    for (const auto& entry : node) {
        const auto key = Scalar(entry.first, path);
        Require(seen.insert(key).second, TStringBuilder() << path << ": duplicate key " << key);
        bool known = false;
        for (const auto item : allowed) {
            known = known || key == item;
        }
        Require(known, TStringBuilder() << path << ": unknown key " << key);
    }
}

ui64 Unsigned(TStringBuf text, TStringBuf path) {
    Require(!text.empty(), TStringBuilder() << path << " must be a positive decimal integer");
    for (const char c : text) {
        Require(c >= '0' && c <= '9', TStringBuilder() << path << " must be a positive decimal integer");
    }
    ui64 result = 0;
    Require(TryFromString(text, result) && result > 0, TStringBuilder() << path << " must be a positive decimal integer");
    return result;
}

ui32 UInt32(const YAML::Node& node, TStringBuf path) {
    const auto value = Unsigned(Scalar(node, path), path);
    Require(value <= std::numeric_limits<ui32>::max(), TStringBuilder() << path << " is too large");
    return value;
}

TDuration Duration(const YAML::Node& node, TStringBuf path) {
    const auto seconds = Unsigned(Scalar(node, path), path);
    Require(seconds <= std::numeric_limits<ui64>::max() / 1000000, TStringBuilder() << path << " is too large");
    return TDuration::Seconds(seconds);
}

TMaybe<TString> OptionalPath(const YAML::Node& node, TStringBuf path) {
    if (!node || node.IsNull()) {
        return Nothing();
    }
    const auto value = Scalar(node, path);
    Require(!value.empty() && value.find('\0') == TString::npos, TStringBuilder() << path << " must be a nonempty file path or null");
    return value;
}

struct THostPort {
    TString Host;
    ui16 Port;
};

THostPort HostPort(TStringBuf address) {
    Require(!address.empty(), "host and port are required");
    for (const char c : address) {
        Require(static_cast<unsigned char>(c) > 32 && static_cast<unsigned char>(c) < 127, "host:port contains whitespace or invalid characters");
    }
    Require(address.find_first_of("/@?#\\%") == TStringBuf::npos, "host:port cannot contain userinfo, path, query, fragment or escapes");
    TStringBuf host;
    TStringBuf port;
    if (address.StartsWith('[')) {
        const auto close = address.find(']');
        Require(close != TStringBuf::npos && close + 1 < address.size() && address[close + 1] == ':', "IPv6 address requires [host]:port");
        host = address.substr(0, close + 1);
        port = address.substr(close + 2);
        in6_addr parsed;
        const TString ip(address.substr(1, close - 1));
        Require(inet_pton(AF_INET6, ip.c_str(), &parsed) == 1, "invalid IPv6 address");
    } else {
        const auto colon = address.find(':');
        Require(colon != TStringBuf::npos && address.find(':', colon + 1) == TStringBuf::npos, "endpoint requires host:port; IPv6 requires brackets");
        host = address.substr(0, colon);
        port = address.substr(colon + 1);
        Require(!host.empty(), "endpoint host is required");
        for (const char c : host) {
            Require((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_', "invalid endpoint host");
        }
    }
    const auto number = Unsigned(port, "port");
    Require(number <= 65535, "port must be in 1..65535");
    return {TString(host), static_cast<ui16>(number)};
}

TString ReadBounded(const TString& path, size_t maxBytes, TStringBuf kind) {
    // Read one byte beyond the bound, including for streams whose size is unknown.
    TFileInput input(path);
    TString result = TString::Uninitialized(maxBytes + 1);
    const size_t size = input.Load(result.Detach(), result.size());
    Require(size <= maxBytes, TStringBuilder() << kind << " exceeds size limit");
    result.resize(size);
    return result;
}

TMaybe<TString> ReadCredential(const TMaybe<TString>& path, TStringBuf kind) {
    if (!path) {
        return Nothing();
    }
    TString value;
    try {
        value = ReadBounded(*path, MaxCredentialBytes, kind);
    } catch (const yexception&) {
        // Do not copy file content, path, or lower-level exception into logs.
        throw yexception() << "cannot load " << kind << " credential file";
    }
    Require(!value.empty(), TStringBuilder() << kind << " credential file is empty");
    Require(value.find('\0') == TString::npos, TStringBuilder() << kind << " credential file contains NUL");
    return value;
}

// The parser enforces every single-field rule; only cross-field constraints remain.
void ValidateConfig(const TConfig& config) {
    TString scheme;
    THashSet<TString> seen;
    for (const auto& [pile, endpoints] : config.Endpoints) {
        for (const auto& endpoint : endpoints) {
            Require(scheme.empty() || scheme == endpoint.Scheme, "mixed grpc/grpcs endpoint schemes are forbidden");
            TString key = endpoint.Host;
            key.to_lower();
            key += ":" + ToString(endpoint.Port);
            Require(seen.insert(key).second, "duplicate endpoint");
            scheme = endpoint.Scheme;
        }
    }
    const auto& auth = config.Auth;
    Require(auth.ClientCertFile.Defined() == auth.ClientKeyFile.Defined(), "cert_file and key_file must be supplied together");
    Require(scheme == "grpcs" || (!auth.CaFile && !auth.ClientCertFile && !auth.ClientKeyFile),
        "ca_file, cert_file and key_file require grpcs:// endpoints");
    // TDuration addition saturates, so the sums below cannot wrap.
    const auto& t = config.Timing;
    Require(t.StatePollInterval + t.ReadTimeout < t.EvidenceTtl, "state_poll_interval_seconds + read_timeout_seconds < evidence_ttl_seconds is required");
    // A busy SelfCheck slot defers the round's sample to the moment the call settles, so consecutive
    // sends on one endpoint are at most max(client deadline, round interval) apart, which this sum
    // bounds below the TTL: the clean window, measured on send times, never breaks on a sampling gap.
    Require(t.SelfCheckClientTimeout + t.RoundInterval < t.EvidenceTtl, "selfcheck_operation_timeout_seconds + 2 + round_interval_seconds < evidence_ttl_seconds is required");
    Require(t.BscWarmup >= TDuration::Seconds(20), "bsc_warmup_seconds >= 20 is required");
}

} // namespace

TEndpoint ParseEndpoint(TStringBuf uri) {
    const auto separator = uri.find("://");
    Require(separator != TStringBuf::npos, "endpoint requires explicit grpc:// or grpcs:// scheme");
    const auto scheme = uri.substr(0, separator);
    Require(scheme == "grpc" || scheme == "grpcs", "endpoint scheme must be grpc or grpcs");
    const auto parsed = HostPort(uri.substr(separator + 3));
    return {TString(uri), TString(scheme), parsed.Host, parsed.Port};
}

TConfig ParseConfig(TStringBuf yaml) {
    Require(yaml.size() <= MaxConfigBytes, "config exceeds size limit");
    TConfig result;
    try {
        const auto documents = YAML::LoadAll(std::string(yaml.data(), yaml.size()));
        Require(documents.size() == 1, "config must contain exactly one YAML document");
        const auto root = documents.front();
        Map(root, "config", {"cluster_id", "mode", "endpoints", "auth", "http", "timing", "health_endpoints_per_pile", "logging"});
        Require(root["cluster_id"].IsDefined(), "cluster_id is required");
        Require(root["endpoints"].IsDefined(), "endpoints is required");
        result.ClusterId = Scalar(root["cluster_id"], "cluster_id");
        Require(!result.ClusterId.empty() && result.ClusterId.find('\0') == TString::npos, "cluster_id must be nonempty and contain no NUL");
        if (const auto node = root["mode"]) {
            const auto mode = Scalar(node, "mode");
            Require(mode == "manual" || mode == "auto", "mode must be manual or auto");
            result.Mode = mode == "auto" ? EMode::Auto : EMode::Manual;
        }
        const auto endpoints = root["endpoints"];
        Require(endpoints.IsMap(), "endpoints must be a mapping");
        for (const auto& entry : endpoints) {
            const auto pile = Scalar(entry.first, "endpoints pile");
            Require(pile.find('\0') == TString::npos, "pile names must contain no NUL");
            const auto [it, inserted] = result.Endpoints.emplace(pile, TVector<TEndpoint>{});
            Require(inserted, "endpoints: duplicate key");
            Require(entry.second.IsSequence(), "endpoints per pile must be a sequence");
            for (const auto& endpoint : entry.second) {
                it->second.push_back(ParseEndpoint(Scalar(endpoint, "endpoints[]")));
            }
            Require(!it->second.empty(), "endpoints per pile must not be empty");
        }
        Require(!result.Endpoints.empty(), "endpoints must configure at least one pile");
        if (const auto node = root["auth"]) {
            Map(node, "auth", {"ca_file", "cert_file", "key_file", "token_file"});
            result.Auth.CaFile = OptionalPath(node["ca_file"], "auth.ca_file");
            result.Auth.ClientCertFile = OptionalPath(node["cert_file"], "auth.cert_file");
            result.Auth.ClientKeyFile = OptionalPath(node["key_file"], "auth.key_file");
            result.Auth.TokenFile = OptionalPath(node["token_file"], "auth.token_file");
        }
        if (const auto node = root["http"]) {
            Map(node, "http", {"listen"});
            if (node["listen"]) {
                result.Http.Listen = Scalar(node["listen"], "http.listen");
                HostPort(result.Http.Listen);
            }
        }
        if (const auto node = root["timing"]) {
            Map(node, "timing", {"state_poll_interval_seconds", "read_timeout_seconds", "round_interval_seconds", "selfcheck_operation_timeout_seconds", "evidence_ttl_seconds", "confirm_seconds", "bsc_warmup_seconds", "mutation_operation_timeout_seconds"});
            auto& t = result.Timing;
            const std::initializer_list<std::pair<TStringBuf, TDuration*>> durations = {
                {"state_poll_interval_seconds", &t.StatePollInterval}, {"read_timeout_seconds", &t.ReadTimeout},
                {"round_interval_seconds", &t.RoundInterval}, {"selfcheck_operation_timeout_seconds", &t.SelfCheckOperationTimeout},
                {"evidence_ttl_seconds", &t.EvidenceTtl}, {"confirm_seconds", &t.Confirm}, {"bsc_warmup_seconds", &t.BscWarmup},
                {"mutation_operation_timeout_seconds", &t.MutationOperationTimeout},
            };
            for (const auto& [key, target] : durations) {
                if (node[std::string(key)]) {
                    *target = Duration(node[std::string(key)], key);
                }
            }
        }
        result.Timing.SelfCheckClientTimeout = result.Timing.SelfCheckOperationTimeout + ClientDeadlineMargin;
        result.Timing.MutationClientTimeout = result.Timing.MutationOperationTimeout + ClientDeadlineMargin;
        if (const auto node = root["health_endpoints_per_pile"]) {
            result.HealthEndpointsPerPile = UInt32(node, "health_endpoints_per_pile");
        }
        if (const auto node = root["logging"]) {
            Map(node, "logging", {"level"});
            if (node["level"]) {
                result.Logging.Level = Scalar(node["level"], "logging.level");
            }
            const auto& level = result.Logging.Level;
            Require(level == "trace" || level == "debug" || level == "info" || level == "warn" || level == "error" || level == "fatal", "unsupported logging.level");
        }
    } catch (const YAML::Exception&) {
        // yaml-cpp exception messages can include source scalar/credential text.
        throw yexception() << "invalid YAML configuration";
    }
    ValidateConfig(result);
    return result;
}

TConfig LoadConfig(const TString& path) {
    return ParseConfig(ReadBounded(path, MaxConfigBytes, "config"));
}

TMaybe<TString> LoadToken(const TMaybe<TString>& file) {
    auto token = ReadCredential(file, "token");
    if (token) {
        // Token files commonly end in a newline. Never transmit that delimiter.
        while (!token->empty() && (token->back() == '\n' || token->back() == '\r')) {
            token->pop_back();
        }
        Require(!token->empty() && token->find_first_of("\r\n") == TString::npos, "token credential must contain one nonempty line");
    }
    return token;
}

TLoadedCredentials LoadCredentials(const TConfig& config) {
    TLoadedCredentials result;
    result.Ca = ReadCredential(config.Auth.CaFile, "CA");
    result.ClientCert = ReadCredential(config.Auth.ClientCertFile, "client certificate");
    result.ClientKey = ReadCredential(config.Auth.ClientKeyFile, "client key");
    result.Token = LoadToken(config.Auth.TokenFile);
    return result;
}

TEntrypointArguments ParseEntrypointArguments(const TVector<TString>& arguments) {
    TEntrypointArguments result;
    THashSet<TString> seen;
    for (size_t i = 0; i < arguments.size(); ++i) {
        TStringBuf argument(arguments[i]);
        const auto equal = argument.find('=');
        const TString key(argument.substr(0, equal));
        Require(seen.insert(key).second, TStringBuilder() << "duplicate argument " << key);
        if (key == "--help" || key == "--version" || key == "--check-config" || key == "--clear-pending-mutation") {
            Require(equal == TStringBuf::npos, TStringBuilder() << key << " does not take a value");
            if (key == "--clear-pending-mutation") {
                result.ClearPendingMutation = true;
            } else {
                Require(result.Action == EEntrypointAction::Run, "only one entrypoint action is allowed");
                result.Action = key == "--help" ? EEntrypointAction::Help : key == "--version" ? EEntrypointAction::Version : EEntrypointAction::CheckConfig;
            }
        } else {
            Require(key == "--config" || key == "--state-dir", TStringBuilder() << "unknown argument " << key);
            TString value;
            if (equal != TStringBuf::npos) {
                value = argument.substr(equal + 1);
            } else {
                Require(i + 1 < arguments.size() && !TStringBuf(arguments[i + 1]).StartsWith("--"), TStringBuilder() << key << " requires a value");
                value = arguments[++i];
            }
            Require(!value.empty() && value.find('\0') == TString::npos, TStringBuilder() << key << " requires a nonempty value");
            if (key == "--config") {
                result.ConfigFile = value;
            } else {
                result.StateDir = value;
            }
        }
    }
    Require(!result.ClearPendingMutation || result.Action == EEntrypointAction::Run,
        "--clear-pending-mutation cannot be combined with --check-config, --help or --version");
    if (result.Action == EEntrypointAction::Run || result.Action == EEntrypointAction::CheckConfig) {
        Require(!result.ConfigFile.empty(), "--config FILE is required");
    }
    if (result.Action == EEntrypointAction::Run) {
        Require(!result.StateDir.empty(), "--state-dir DIR is required");
    }
    return result;
}

TStringBuf EntrypointHelp() {
    return "bridge_skipper --config FILE --state-dir DIR [--clear-pending-mutation]\n"
           "bridge_skipper --config FILE --check-config\n"
           "  --config FILE             Standalone YAML configuration (loaded once)\n"
           "  --state-dir DIR           Local state directory, keep it private; ignored by --check-config\n"
           "  --check-config            Validate configuration and credentials; no listeners or RPCs\n"
           "  --clear-pending-mutation  Clear pending mutation before starting the controller\n"
           "  --version                 Print build and state schema versions\n"
           "  --help                    Show this help\n";
}

} // namespace NBridgeSkipper
