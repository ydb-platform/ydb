#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <ydb/core/security/certificate_check/cert_auth_processor.h>
#include <ydb/core/security/certificate_check/cert_auth_utils.h>
#include <ydb/core/security/certificate_check/cert_check.h>
#include <ydb/core/security/certificate_check/test_utils/test_cert_auth_utils.h>

#include <unordered_map>
#include <vector>

namespace {

struct TStaticCertificates {
    TTempFile ServerFile;
    TString ValidClientCert;
    TString OtherClientCert;
};

const TStaticCertificates& GetStaticCertificates() {
    static const TStaticCertificates certificates = [] {
        using namespace NKikimr::NCertTestUtils;

        TProps caProps = TProps::AsCA();
        caProps.WithValid(TDuration::Hours(1));
        TProps serverProps = TProps::AsServer();
        serverProps.WithValid(TDuration::Hours(1));
        TProps clientProps = TProps::AsClient();
        clientProps.WithValid(TDuration::Hours(1));

        TProps otherCaProps = TProps::AsCA();
        otherCaProps.WithValid(TDuration::Hours(1));
        TProps otherClientProps = TProps::AsClient();
        otherClientProps.WithValid(TDuration::Hours(1));

        const auto ca = GenerateCA(caProps);
        const auto server = GenerateSignedCert(ca, serverProps);
        const auto client = GenerateSignedCert(ca, clientProps);
        const auto otherCa = GenerateCA(otherCaProps);
        const auto otherClient = GenerateSignedCert(otherCa, otherClientProps);

        TTempFile serverFile(MakeTempName(nullptr, "ydb_cert_fuzz", "pem"));
        TFileOutput out(serverFile.Name());
        out.Write(server.Certificate.data(), server.Certificate.size());
        out.Finish();

        return TStaticCertificates{
            .ServerFile = std::move(serverFile),
            .ValidClientCert = TString(client.Certificate),
            .OtherClientCert = TString(otherClient.Certificate),
        };
    }();
    return certificates;
}

const TStringBuf* PickShortName(FuzzedDataProvider& fdp) {
    static const TStringBuf names[] = {"C", "ST", "L", "O", "OU", "CN"};
    return &names[fdp.ConsumeIntegralInRange<size_t>(0, sizeof(names) / sizeof(names[0]) - 1)];
}

NKikimrConfig::TClientCertificateAuthorization BuildAuthConfig(FuzzedDataProvider& fdp) {
    NKikimrConfig::TClientCertificateAuthorization auth;
    auth.SetDefaultGroup(NAuthSecurityFuzz::ConsumeToken(fdp, 24));
    auth.SetRequestClientCertificate(fdp.ConsumeBool());

    for (size_t i = 0, defs = fdp.ConsumeIntegralInRange<size_t>(0, 4); i < defs; ++i) {
        auto* definition = auth.AddClientCertificateDefinitions();
        definition->SetCanCheckNodeHostByCN(fdp.ConsumeBool());
        definition->SetRequireSameIssuer(fdp.ConsumeBool());

        for (size_t j = 0, terms = fdp.ConsumeIntegralInRange<size_t>(0, 4); j < terms; ++j) {
            TVector<TString> values;
            TVector<TString> suffixes;
            for (size_t k = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 3); k < count; ++k) {
                values.push_back(NAuthSecurityFuzz::ConsumeToken(fdp, 24));
            }
            for (size_t k = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 3); k < count; ++k) {
                suffixes.push_back(NAuthSecurityFuzz::ConsumeToken(fdp, 24));
            }
            *definition->AddSubjectTerms() = NKikimr::MakeSubjectTerm(TString(*PickShortName(fdp)), values, suffixes);
        }

        if (fdp.ConsumeBool()) {
            auto* subjectDns = definition->MutableSubjectDns();
            for (size_t j = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 3); j < count; ++j) {
                *subjectDns->AddValues() = NAuthSecurityFuzz::ConsumeToken(fdp, 24);
            }
            for (size_t j = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 3); j < count; ++j) {
                *subjectDns->AddSuffixes() = NAuthSecurityFuzz::ConsumeToken(fdp, 24);
            }
        }

        for (size_t j = 0, groups = fdp.ConsumeIntegralInRange<size_t>(0, 3); j < groups; ++j) {
            *definition->AddMemberGroups() = NAuthSecurityFuzz::ConsumeToken(fdp, 24);
        }
    }

    return auth;
}

void TryReaderFunctions(const TString& certPem) {
    auto x509 = NKikimr::X509CertificateReader::ReadCertAsPEM(certPem);
    if (!x509) {
        return;
    }

    const auto subjectTerms = NKikimr::X509CertificateReader::ReadSubjectTerms(x509);
    (void)NKikimr::X509CertificateReader::ReadAllSubjectTerms(x509);
    (void)NKikimr::X509CertificateReader::ReadIssuerTerms(x509);
    (void)NKikimr::X509CertificateReader::ReadSubjectDns(x509, subjectTerms);
    (void)NKikimr::X509CertificateReader::GetFingerprint(x509);
}

void TryAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization& auth, const TString& certPem) {
    auto x509 = NKikimr::X509CertificateReader::ReadCertAsPEM(certPem);
    if (!x509) {
        return;
    }

    std::unordered_map<TString, std::vector<TString>> subjectDescription;
    for (const auto& [attribute, value] : NKikimr::X509CertificateReader::ReadAllSubjectTerms(x509)) {
        subjectDescription[attribute].push_back(value);
    }
    const auto dns = NKikimr::X509CertificateReader::ReadSubjectDns(x509, NKikimr::X509CertificateReader::ReadSubjectTerms(x509));

    for (const auto& params : NKikimr::GetCertificateAuthorizationParams(auth)) {
        (void)params.CheckSubject(subjectDescription, dns);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    const TString rawCert(reinterpret_cast<const char*>(data), size);
    const auto& staticCerts = GetStaticCertificates();

    try {
        TryReaderFunctions(rawCert);
    } catch (...) {
    }
    try {
        TryReaderFunctions(staticCerts.ValidClientCert);
    } catch (...) {
    }
    try {
        (void)NKikimr::GetCertificateFingerprint(std::string(rawCert.data(), rawCert.size()));
    } catch (...) {
    }
    try {
        (void)NKikimr::GetCertificateFingerprint(std::string(staticCerts.ValidClientCert.data(), staticCerts.ValidClientCert.size()));
    } catch (...) {
    }

    const auto auth = BuildAuthConfig(fdp);
    try {
        TryAuthorizationParams(auth, staticCerts.ValidClientCert);
    } catch (...) {
    }

    try {
        NKikimr::TCertificateAuthValues values;
        values.ClientCertificateAuthorization = auth;
        values.ServerCertificateFilePath = staticCerts.ServerFile.Name();
        values.Domain = NAuthSecurityFuzz::ConsumeToken(fdp, 16);

        NKikimr::TCertificateChecker checker(values);
        const int mode = fdp.ConsumeIntegralInRange<int>(0, 2);
        if (mode == 0) {
            (void)checker.Check(staticCerts.ValidClientCert);
        } else if (mode == 1) {
            (void)checker.Check(staticCerts.OtherClientCert);
        } else {
            (void)checker.Check(rawCert);
        }
    } catch (...) {
    }

    return 0;
}
