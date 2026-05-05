#include "cert_auth_utils.h"

namespace NKikimr {

std::vector<TCertificateAuthorizationParams> GetCertificateAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization& clientCertificateAuth) {
    std::vector<TCertificateAuthorizationParams> certAuthParams;
    certAuthParams.reserve(clientCertificateAuth.ClientCertificateDefinitionsSize());

    for (const auto& clientCertificateDefinition : clientCertificateAuth.GetClientCertificateDefinitions()) {
        TCertificateAuthorizationParams::TDN dn;
        for (const auto& term: clientCertificateDefinition.GetSubjectTerms()) {
            auto rdn = TCertificateAuthorizationParams::TRDN(term.GetShortName());
            for (const auto& value: term.GetValues()) {
                rdn.AddValue(value);
            }
            for (const auto& suffix: term.GetSuffixes()) {
                rdn.AddSuffix(suffix);
            }
            dn.AddRDN(std::move(rdn));
        }
        std::optional<TCertificateAuthorizationParams::TRDN> subjectDns;
        if (const auto& subjectDnsCfg = clientCertificateDefinition.GetSubjectDns(); subjectDnsCfg.ValuesSize() || subjectDnsCfg.SuffixesSize()) {
            TCertificateAuthorizationParams::TRDN& dns = subjectDns.emplace(TString());
            for (const auto& value: subjectDnsCfg.GetValues()) {
                dns.AddValue(value);
            }
            for (const auto& suffix: subjectDnsCfg.GetSuffixes()) {
                dns.AddSuffix(suffix);
            }
        }
        if (dn || subjectDns) {
            std::vector<TString> groups(clientCertificateDefinition.GetMemberGroups().cbegin(), clientCertificateDefinition.GetMemberGroups().cend());
            certAuthParams.emplace_back(std::move(dn), std::move(subjectDns), clientCertificateDefinition.GetRequireSameIssuer(), std::move(groups));
        }
    }

    return certAuthParams;
}

NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm MakeSubjectTerm(const TString& name, const TVector<TString>& values, const TVector<TString>& suffixes) {
    NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm term;
    term.SetShortName(name);
    for (const auto& val: values) {
        *term.MutableValues()->Add() = val;
    }
    for (const auto& suf: suffixes) {
        *term.MutableSuffixes()->Add() = suf;
    }
    return term;
}

std::string GetCertificateFingerprint(const std::string& certificate) {
    const static std::string defaultFingerprint = "certificate";
    X509CertificateReader::X509Ptr x509Cert = X509CertificateReader::ReadCertAsPEM(certificate);
    if (!x509Cert) {
        return defaultFingerprint;
    }
    std::string fingerprint = X509CertificateReader::GetFingerprint(x509Cert);
    return (fingerprint.empty() ? defaultFingerprint : fingerprint);
}

}  //namespace NKikimr
