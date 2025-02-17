#pragma once

#include <ydb/core/base/ticket_parser.h>

#include "cert_auth_utils.h"

namespace NKikimr {

class TCertificateChecker {
public:
    struct TCertificateCheckResult {
        TEvTicketParser::TError Error;
        TString UserSid;
        std::vector<TString> Groups;
    };

private:
    struct TPemCertificates {
        X509CertificateReader::X509Ptr ClientCertX509;
        X509CertificateReader::X509Ptr ServerCertX509;
    };

    struct TReadCertificateAsPemResult {
        TEvTicketParser::TError Error;
        TPemCertificates PemCertificates;
    };

    struct TReadClientSubjectResult {
        std::vector<std::pair<TString, TString>> SubjectDn;
        std::vector<TString> SubjectDns; // Subject alternative names, DNS
        TEvTicketParser::TError Error;
    };

    const std::vector<TCertificateAuthorizationParams> CertificateAuthorizationParams;
    const TString ServerCertificate;
    const TString Domain;
    const TString DefaultGroup;

public:
    TCertificateChecker(const TCertificateAuthValues& certificateAuthValues);

    TCertificateCheckResult Check(const TString& clientCertificate) const;

private:
    static TString ReadFile(const TString& fileName);

    TReadCertificateAsPemResult ReadCertificatesAsPem(const TString& clientCertificate) const;
    TEvTicketParser::TError CheckIssuers(const TPemCertificates& pemCertificates) const;
    TReadClientSubjectResult ReadSubjectFromClientCertificate(const TPemCertificates& pemCertificates) const;
    TString CreateUserSidFromSubjectDn(const std::vector<std::pair<TString, TString>>& subjectDn) const;
    TEvTicketParser::TError CheckClientSubject(const TReadClientSubjectResult& subjectInfo, const TCertificateAuthorizationParams& authParams) const;
    TCertificateCheckResult DefaultCheckClientCertificate(const TPemCertificates& pemCertificates) const;
    TCertificateCheckResult CheckClientCertificate(const TPemCertificates& pemCertificates) const;
    TString GetDefaultGroup() const;
};

} // namespace Nkikimr
