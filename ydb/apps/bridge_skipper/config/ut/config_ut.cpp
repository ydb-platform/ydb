#include <ydb/apps/bridge_skipper/config/config.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/tempfile.h>

namespace NBridgeSkipper {
namespace {

TString Minimal(TStringBuf extra = {}) {
    return TStringBuilder() << "cluster_id: test-bridge\nendpoints: {dc-a: [grpcs://node.example:2135]}\n" << extra;
}

void Reject(TStringBuf yaml, TStringBuf error) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(ParseConfig(yaml), yexception, error);
}

} // namespace

Y_UNIT_TEST_SUITE(TSkipperConfiguration) {
    Y_UNIT_TEST(DefaultsMatchDocumentedBudgets) {
        const auto config = ParseConfig(Minimal());
        UNIT_ASSERT_VALUES_EQUAL(config.ClusterId, "test-bridge");
        UNIT_ASSERT_VALUES_EQUAL(config.Http.Listen, "127.0.0.1:8080");
        UNIT_ASSERT_VALUES_EQUAL(config.HealthEndpointsPerPile, 2);
        const auto& t = config.Timing;
        UNIT_ASSERT_VALUES_EQUAL(t.StatePollInterval, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(t.ReadTimeout, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(t.RoundInterval, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(t.SelfCheckOperationTimeout, TDuration::Seconds(12));
        UNIT_ASSERT_VALUES_EQUAL(t.SelfCheckClientTimeout, TDuration::Seconds(14));
        UNIT_ASSERT_VALUES_EQUAL(t.EvidenceTtl, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(t.Confirm, TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(t.BscWarmup, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(t.MutationOperationTimeout, TDuration::Seconds(25));
        UNIT_ASSERT_VALUES_EQUAL(t.MutationClientTimeout, TDuration::Seconds(27));
        UNIT_ASSERT(!config.Auth.TokenFile);
    }

    Y_UNIT_TEST(ShippedExampleIsValid) {
        // The same loading and validation as --check-config.
        const auto config = LoadConfig(ArcadiaFromCurrentLocation(__SOURCE_FILE__, "../example.yaml"));
        const auto credentials = LoadCredentials(config);
        UNIT_ASSERT(!credentials.Token);
        UNIT_ASSERT_VALUES_EQUAL(config.ClusterId, "production-bridge-01");
        UNIT_ASSERT_VALUES_EQUAL(config.Endpoints.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(config.Timing.Confirm, TDuration::Seconds(20));
    }

    Y_UNIT_TEST(ClientDeadlinesAreDerivedFromOperationTimeouts) {
        const auto config = ParseConfig(Minimal("timing: {selfcheck_operation_timeout_seconds: 7, mutation_operation_timeout_seconds: 10}\n"));
        UNIT_ASSERT_VALUES_EQUAL(config.Timing.SelfCheckClientTimeout, TDuration::Seconds(9));
        UNIT_ASSERT_VALUES_EQUAL(config.Timing.MutationClientTimeout, TDuration::Seconds(12));
    }

    Y_UNIT_TEST(PileEndpointsAndNestedOverrides) {
        const auto config = ParseConfig(
            "cluster_id: test\nendpoints: {dc-a: [grpcs://a:1, grpcs://a:2], '': [grpcs://b:1]}\n"
            "auth: {ca_file: null, cert_file: cert.pem, key_file: key.pem, token_file: null}\n"
            "http: {listen: '[::1]:8081'}\nhealth_endpoints_per_pile: 3\nlogging: {level: debug}\n");
        UNIT_ASSERT_VALUES_EQUAL(config.Endpoints.at("dc-a").size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(config.Endpoints.at("").front().Uri, "grpcs://b:1");
        UNIT_ASSERT_VALUES_EQUAL(*config.Auth.ClientCertFile, "cert.pem");
        UNIT_ASSERT(!config.Auth.CaFile);
        UNIT_ASSERT_VALUES_EQUAL(config.HealthEndpointsPerPile, 3);
        UNIT_ASSERT_VALUES_EQUAL(config.Logging.Level, "debug");
    }

    Y_UNIT_TEST(RejectsMissingIdentityEndpointsAndInvalidMode) {
        Reject("endpoints: {a: [grpc://node:1]}\n", "cluster_id is required");
        Reject("cluster_id: ''\nendpoints: {a: [grpc://node:1]}\n", "cluster_id must be nonempty");
        Reject("cluster_id: cluster\n", "endpoints is required");
        Reject("cluster_id: cluster\nendpoints: {}\n", "at least one pile");
        Reject("cluster_id: cluster\nendpoints: {a: []}\n", "must not be empty");
        Reject(Minimal("mode: other\n"), "unknown key");
    }

    Y_UNIT_TEST(StrictYamlShapeDuplicatesAndLimits) {
        Reject(Minimal("cluster_id: duplicate\n"), "duplicate key");
        Reject(Minimal("timing: {read_timeout_seconds: 1, read_timeout_seconds: 2}\n"), "duplicate key");
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1], a: [grpc://b:1]}\n", "duplicate key");
        Reject(Minimal("auth: {token_file: null, token_file: null}\n"), "duplicate key");
        Reject(Minimal("auth: {unknown: secret}\n"), "unknown key");
        Reject(Minimal("unknown: 1\n"), "unknown key");
        Reject(Minimal("http: null\n"), "must be a mapping");
        Reject(Minimal("timing: []\n"), "must be a mapping");
        Reject("cluster_id: c\nendpoints: [grpc://a:1]\n", "must be a mapping");
        Reject("cluster_id: c\nendpoints: {a: grpc://a:1}\n", "must be a sequence");
        Reject(Minimal("logging: {level: []}\n"), "must be a scalar");
        Reject(Minimal() + "---\n" + Minimal(), "exactly one YAML document");
        Reject("{bad: [yaml", "invalid YAML");
        Reject("[]", "must be a mapping");
        Reject(TString(1024 * 1024 + 1, 'x'), "size limit");
        TTempFileHandle file;
        TFileOutput(file.Name()).Write(Minimal());
        UNIT_ASSERT_VALUES_EQUAL(LoadConfig(file.Name()).ClusterId, "test-bridge");
    }

    Y_UNIT_TEST(EndpointParserPreservesAuthorityAndRejectsUnsafeUris) {
        const auto endpoint = ParseEndpoint("grpcs://[2001:db8::1]:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoint.Uri, "grpcs://[2001:db8::1]:2135");
        UNIT_ASSERT_VALUES_EQUAL(endpoint.Host, "[2001:db8::1]");
        UNIT_ASSERT_VALUES_EQUAL(endpoint.Port, 2135);
        UNIT_ASSERT_VALUES_EQUAL(ParseEndpoint("grpc://node.example:1").Port, 1);
        UNIT_ASSERT_VALUES_EQUAL(ParseEndpoint("grpc://127.0.0.1:65535").Port, 65535);
        for (const auto uri : {"node:2135", "http://node:2135", "grpc://node", "grpc://:2135", "grpc://node:",
                "grpc://node:0", "grpc://node:65536", "grpc://node:+1", "grpc://node:1.5", "grpc://user@node:2135",
                "grpc://node:2135/", "grpc://node:2135?x=1", "grpc://node:2135#fragment", "grpc://node:2135\\path",
                "grpc://2001:db8::1:2135", "grpc://[bad-ip]:2135", "grpc://[::1]", "grpc://node name:2135",
                "grpc://node%20name:2135", "grpc://[::1]:1:2"}) {
            UNIT_ASSERT_EXCEPTION(ParseEndpoint(uri), yexception);
        }
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1], b: [grpcs://b:1]}\n", "mixed grpc/grpcs");
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1, grpc://a:1]}\n", "duplicate endpoint");
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1], b: [grpc://a:1]}\n", "duplicate endpoint");
        Reject("cluster_id: c\nendpoints: {a: [grpc://Node.Example:1, grpc://node.example:1]}\n", "duplicate endpoint");
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1], b: [grpc://A:1]}\n", "duplicate endpoint");
        Reject("cluster_id: c\nendpoints: {a: [grpc://a:1, grpc://a:01]}\n", "duplicate endpoint");
    }

    Y_UNIT_TEST(EveryTimingConstraintHasBoundaryCoverage) {
        Reject(Minimal("timing: {read_timeout_seconds: 29}\n"), "state_poll_interval_seconds + read_timeout_seconds < evidence_ttl_seconds");
        Reject(Minimal("timing: {state_poll_interval_seconds: 25}\n"), "state_poll_interval_seconds + read_timeout_seconds < evidence_ttl_seconds");
        Reject(Minimal("timing: {selfcheck_operation_timeout_seconds: 18}\n"), "selfcheck_operation_timeout_seconds + 2 + round_interval_seconds < evidence_ttl_seconds");
        Reject(Minimal("timing: {evidence_ttl_seconds: 24}\n"), "selfcheck_operation_timeout_seconds + 2 + round_interval_seconds < evidence_ttl_seconds");
        Reject(Minimal("timing: {bsc_warmup_seconds: 19}\n"), "bsc_warmup_seconds >= 20");
        UNIT_ASSERT_NO_EXCEPTION(ParseConfig(Minimal(
            "timing: {read_timeout_seconds: 28, selfcheck_operation_timeout_seconds: 17, "
            "confirm_seconds: 10, bsc_warmup_seconds: 20, mutation_operation_timeout_seconds: 30}\n")));
        UNIT_ASSERT_NO_EXCEPTION(ParseConfig(Minimal("timing: {state_poll_interval_seconds: 24}\n")));
    }

    Y_UNIT_TEST(NumericAndTypedValidationCannotBeBypassed) {
        for (const auto value : {"0", "-1", "1.5", "null", "true", "+1", "18446744073709551616", "18446744073709551615"}) {
            UNIT_ASSERT_EXCEPTION(ParseConfig(Minimal(TStringBuilder() << "timing: {state_poll_interval_seconds: " << value << "}\n")), yexception);
        }
        Reject(Minimal("health_endpoints_per_pile: 0\n"), "positive decimal integer");
        Reject(Minimal("health_endpoints_per_pile: 4294967296\n"), "too large");
        Reject(Minimal("logging: {level: unknown}\n"), "unsupported logging.level");
    }

    Y_UNIT_TEST(AuthPairing) {
        UNIT_ASSERT_NO_EXCEPTION(ParseConfig("cluster_id: c\nendpoints: {a: [grpc://node:1]}\nauth: {token_file: token}\n"));
        Reject(Minimal("auth: {cert_file: cert}\n"), "must be supplied together");
        Reject(Minimal("auth: {key_file: key}\n"), "must be supplied together");
        Reject(Minimal("auth: {ca_file: ''}\n"), "nonempty file path");
        Reject("cluster_id: c\nendpoints: {a: [grpc://node:1]}\nauth: {ca_file: ca}\n", "require grpcs://");
        Reject("cluster_id: c\nendpoints: {a: [grpc://node:1]}\nauth: {cert_file: cert, key_file: key}\n", "require grpcs://");
        UNIT_ASSERT_NO_EXCEPTION(ParseConfig(Minimal("auth: {ca_file: ca, cert_file: cert, key_file: key}\n")));
    }

    Y_UNIT_TEST(CredentialsRemainBoundedAndReadErrorsRedacted) {
        TTempFileHandle token;
        TFileOutput(token.Name()).Write("secret-token\r\n");
        auto config = ParseConfig(Minimal());
        config.Auth.TokenFile = token.Name();
        const auto loaded = LoadCredentials(config);
        UNIT_ASSERT_VALUES_EQUAL(*loaded.Token, "secret-token");
        TFileOutput(token.Name()).Write("replacement");
        UNIT_ASSERT_VALUES_EQUAL(*loaded.Token, "secret-token");
        UNIT_ASSERT_VALUES_EQUAL(*LoadToken(config.Auth.TokenFile), "replacement");
        for (const auto& value : {TString(), TString("one\ntwo\n"), TString("a\0b", 3), TString(16 * 1024 * 1024 + 1, 'x')}) {
            TFileOutput(token.Name()).Write(value);
            UNIT_ASSERT_EXCEPTION(LoadCredentials(config), yexception);
        }
        config.Auth.TokenFile = "/does/not/exist/secret-token-file";
        try {
            LoadCredentials(config);
            UNIT_FAIL("missing credential file was accepted");
        } catch (const yexception& e) {
            const TStringBuf message(e.what());
            UNIT_ASSERT(message.Contains("cannot load token credential file"));
            UNIT_ASSERT(!message.Contains("/does/not/exist"));
            UNIT_ASSERT(!message.Contains("secret-token-file"));
        }
    }

    Y_UNIT_TEST(LoadsAllCredentialKinds) {
        TTempFileHandle ca;
        TTempFileHandle certificate;
        TTempFileHandle key;
        TFileOutput(ca.Name()).Write("ca-material");
        TFileOutput(certificate.Name()).Write("certificate-material");
        TFileOutput(key.Name()).Write("key-material");
        auto config = ParseConfig(Minimal());
        config.Auth.CaFile = ca.Name();
        config.Auth.ClientCertFile = certificate.Name();
        config.Auth.ClientKeyFile = key.Name();
        const auto credentials = LoadCredentials(config);
        UNIT_ASSERT_VALUES_EQUAL(*credentials.Ca, "ca-material");
        UNIT_ASSERT_VALUES_EQUAL(*credentials.ClientCert, "certificate-material");
        UNIT_ASSERT_VALUES_EQUAL(*credentials.ClientKey, "key-material");
    }

    Y_UNIT_TEST(HttpListenSyntax) {
        for (const auto listen : {"localhost:8080", "[::1]:8080", "0.0.0.0:8080", "[::]:8080", "host.example:8080"}) {
            UNIT_ASSERT_NO_EXCEPTION(ParseConfig(Minimal(TStringBuilder() << "http: {listen: '" << listen << "'}\n")));
        }
        for (const auto listen : {"::1:8080", "127.0.0.1", "127.0.0.1:0", "http://localhost:8080", "[bad]:8080"}) {
            UNIT_ASSERT_EXCEPTION(ParseConfig(Minimal(TStringBuilder() << "http: {listen: '" << listen << "'}\n")), yexception);
        }
    }

    Y_UNIT_TEST(HttpTrustedAuthoritiesPreserveListenerFormsAndExplicitProxyNames) {
        const auto defaults = ParseConfig(Minimal());
        UNIT_ASSERT(defaults.Http.TrustedAuthorities.empty());
        for (const auto listen : {"127.0.0.1:8080", "localhost:8080", "[::1]:8080", "0.0.0.0:8080", "[::]:8080", "host.example:8080"}) {
            const auto config = ParseConfig(Minimal(TStringBuilder()
                << "http: {listen: '" << listen << "', trusted_authorities: ['proxy.example', 'proxy.example:8443', '[2001:db8::1]:443']}\n"));
            UNIT_ASSERT_VALUES_EQUAL(config.Http.Listen, listen);
            UNIT_ASSERT_VALUES_EQUAL(config.Http.TrustedAuthorities.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(config.Http.TrustedAuthorities[0], "proxy.example");
        }
        UNIT_ASSERT(ParseConfig(Minimal("http: {trusted_authorities: []}\n")).Http.TrustedAuthorities.empty());
    }

    Y_UNIT_TEST(HttpTrustedAuthoritiesRejectMalformedOrAmbiguousConfiguration) {
        for (const auto value : {"null", "proxy.example", "{}"}) {
            Reject(Minimal(TStringBuilder() << "http: {trusted_authorities: " << value << "}\n"), "must be a sequence");
        }
        Reject(Minimal("http: {trusted_authorities: [{}]}\n"), "must be a scalar");
        for (const auto authority : {"", "*", "*.example", "http://proxy.example", "user@proxy.example", "proxy.example/path",
                "proxy.example?query", "proxy.example#fragment", "proxy.example%2e", "proxy.example:0", "proxy.example:65536",
                "proxy.example:", "proxy.example:abc", "proxy.example:80:90", "::1", "[bad]", "[::1]suffix", "proxy.example,evil.example", "proxy example"})
        {
            Reject(Minimal(TStringBuilder() << "http: {trusted_authorities: ['" << authority << "']}\n"), "invalid http.trusted_authorities entry");
        }
        for (const auto entries : {"'Proxy.Example:8080', 'proxy.example:08080'", "'[::1]:8080', '[0:0:0:0:0:0:0:1]:8080'"}) {
            Reject(Minimal(TStringBuilder() << "http: {trusted_authorities: [" << entries << "]}\n"), "duplicate http.trusted_authorities entry");
        }
    }

    Y_UNIT_TEST(EntrypointOptionsAndCheckConfig) {
        const auto args = ParseEntrypointArguments({"--config", "config.yaml", "--state-dir=/state", "--check-config"});
        UNIT_ASSERT(args.Action == EEntrypointAction::CheckConfig);
        UNIT_ASSERT_VALUES_EQUAL(args.ConfigFile, "config.yaml");
        UNIT_ASSERT_VALUES_EQUAL(args.StateDir, "/state");
        UNIT_ASSERT(!args.ClearPendingMutation);
        UNIT_ASSERT(ParseEntrypointArguments({"--help"}).Action == EEntrypointAction::Help);
        UNIT_ASSERT(ParseEntrypointArguments({"--version"}).Action == EEntrypointAction::Version);
        UNIT_ASSERT(ParseEntrypointArguments({"--config=c", "--state-dir=s", "--clear-pending-mutation"}).ClearPendingMutation);
        UNIT_ASSERT(EntrypointHelp().Contains("--clear-pending-mutation"));
        for (const TVector<TString>& bad : {
                TVector<TString>{}, {"--check-config"}, {"--config", "c"},
                {"--config=c", "--state-dir=s", "--member-id=m"}, {"--token", "secret"},
                {"--config", "--state-dir", "s"}, {"--config="}, {"--help=true"},
                {"--help", "--version"}, {"--help", "--help"}, {"--clear-pending-mutation=true"},
                {"--config=c", "--state-dir=s", "--clear-pending-mutation", "--clear-pending-mutation"}}) {
            UNIT_ASSERT_EXCEPTION(ParseEntrypointArguments(bad), yexception);
        }
        // Only Run needs a state directory; a read-only action never drops the clear.
        UNIT_ASSERT_VALUES_EQUAL(ParseEntrypointArguments({"--config=c", "--check-config"}).StateDir, "");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseEntrypointArguments({"--config=c"}), yexception, "--state-dir DIR is required");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseEntrypointArguments({"--config=c", "--state-dir=s", "--check-config",
            "--clear-pending-mutation"}), yexception, "cannot be combined");
    }
}

} // namespace NBridgeSkipper
