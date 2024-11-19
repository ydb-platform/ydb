#include "cert_check.h"
#include "cert_auth_utils.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>

namespace NKikimr {

TTempFile SaveToTempFile(const std::string& content, const char* prefix = "cert") {
    TTempFile file = MakeTempName(nullptr, prefix);
    TUnbufferedFileOutput(file.Name()).Write(content);
    return file;
}

Y_UNIT_TEST_SUITE(TCertificateCheckerTest) {
    Y_UNIT_TEST(CheckSubjectDns) {
        using TTestSubjectTerm = std::pair<TString, std::vector<TString>>;
        struct TTestSubjectDnsData {
            TString CommonName = "localhost";
            std::vector<std::string> AltNames;
            std::vector<TString> DnsValues;
            std::vector<TString> DnsSuffixes;
            std::optional<TTestSubjectTerm> SubjectTerm; // one is enough, because we test DNS now
            bool CheckResult = false;
        };

        std::vector<TTestSubjectDnsData> tests = {
            {
                .AltNames = {
                    "IP:1.2.3.4",
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .DnsSuffixes = {
                    ".cluster.net",
                },
                .CheckResult = true,
            },
            {
                .AltNames = {
                    "IP:1.2.3.4", // not DNS
                },
                .DnsValues = {
                    "1.2.3.4",
                },
                .CheckResult = false,
            },
            {
                .CommonName = "the.only.name.net", // CN is also FQDN
                .DnsValues = {
                    "the.only.name.net",
                },
                .CheckResult = true,
            },
            {
                .CommonName = "the.only.name.net", // CN is also FQDN
                .DnsSuffixes = {
                    ".name.net",
                    ".some.other.domain.net",
                },
                .CheckResult = true,
            },
            {
                .CommonName = "", // no DNS in cert
                .DnsSuffixes = {
                    ".cluster.net",
                },
                .CheckResult = false,
            },
            {
                .CommonName = "", // no DNS in cert
                .DnsValues = {
                    "node-1.cluster.net",
                },
                .CheckResult = false,
            },
            {
                // Complex matching
                .AltNames = {
                    "IP:1.2.3.4",
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                    "DNS:my-host.us",
                },
                .DnsValues = {
                    "hello.su",
                    "balancer.cluster.net",
                },
                .DnsSuffixes = {
                    ".123.us",
                    ".cluster-0.net",
                    ".cluster-1.net",
                },
                .CheckResult = true,
            },
            {
                // Complex matching
                .AltNames = {
                    "IP:1.2.3.4",
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                    "DNS:my-host.us",
                },
                .DnsValues = {
                    "hello.su",
                    "no-name",
                },
                .DnsSuffixes = {
                    ".123.us",
                    ".cluster-0.net",
                    ".cluster-1.net",
                    "my-host.us",
                },
                .CheckResult = true,
            },
            {
                // Additional conditions
                // No DNS
                // Subject OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "L",
                    {"TLV", "MSK"},
                },
                .CheckResult = true,
            },
            {
                // Additional conditions
                // No DNS
                // Subject not OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "O",
                    {"Google", "Meta"},
                },
                .CheckResult = false,
            },
            {
                // Additional conditions
                // DNS OK
                // Subject OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .DnsValues = {
                    "node-1.cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "L",
                    {"TLV", "MSK"},
                },
                .CheckResult = true,
            },
            {
                // Additional conditions
                // DNS not OK
                // Subject OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .DnsSuffixes = {
                    ".my-cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "L",
                    {"TLV", "MSK"},
                },
                .CheckResult = false,
            },
            {
                // Additional conditions
                // DNS not OK
                // Subject not OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                },
                .DnsSuffixes = {
                    ".my-cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "O",
                    {"Google", "Meta"},
                },
                .CheckResult = false,
            },
            {
                // Additional conditions
                // DNS OK
                // Subject not OK
                .AltNames = {
                    "DNS:other.name.net",
                    "DNS:node-1.cluster.net",
                    "DNS:*.cluster.net",
                    "DNS:balancer.cluster.net",
                    "DNS:balancer.my-cluster.net",
                },
                .DnsSuffixes = {
                    ".my-cluster.net",
                },
                .SubjectTerm = TTestSubjectTerm{
                    "O",
                    {"Google", "Meta"},
                },
                .CheckResult = false,
            },
        };

        TCertAndKey ca = GenerateCA(TProps::AsCA());

        for (size_t testNumber = 0; testNumber < tests.size(); ++testNumber) {
            const TTestSubjectDnsData& test = tests[testNumber];
            TProps props = TProps::AsClientServer();
            props.CommonName = test.CommonName;
            props.AltNames = test.AltNames;
            TCertAndKey clientServer = GenerateSignedCert(ca, props);
            VerifyCert(clientServer.Certificate, ca.Certificate);

            TCertificateAuthValues opts;
            opts.Domain = "cert";
            TTempFile serverCert = SaveToTempFile(clientServer.Certificate);
            opts.ServerCertificateFilePath = serverCert.Name();
            auto* defs = opts.ClientCertificateAuthorization.AddClientCertificateDefinitions();
            defs->AddMemberGroups("ClusterNodeGroup@cert");

            if (!test.DnsValues.empty() || !test.DnsSuffixes.empty()) {
                auto* dnsCondition = defs->MutableSubjectDns();
                for (const TString& v : test.DnsValues) {
                    dnsCondition->AddValues(v);
                }
                for (const TString& s : test.DnsSuffixes) {
                    dnsCondition->AddSuffixes(s);
                }
            }
            if (test.SubjectTerm) {
                auto* t = defs->AddSubjectTerms();
                t->SetShortName(test.SubjectTerm->first);
                for (const TString& v : test.SubjectTerm->second) {
                    t->AddValues(v);
                }
            }
            TCertificateChecker checker(opts);

            TCertificateChecker::TCertificateCheckResult result = checker.Check(TString(clientServer.Certificate));
            if (test.CheckResult) {
                UNIT_ASSERT_C(result.Error.empty(), "Test number: " << testNumber << ". Error: " << result.Error);
                UNIT_ASSERT_VALUES_EQUAL_C(result.Groups.size(), 1, "Test number: " << testNumber);
                UNIT_ASSERT_VALUES_EQUAL_C(result.Groups[0], "ClusterNodeGroup@cert", "Test number: " << testNumber);
            } else {
                UNIT_ASSERT_C(!result.Error.empty(), "Test number: " << testNumber);
            }
        }
    }
}

} // namespace NKikimr
