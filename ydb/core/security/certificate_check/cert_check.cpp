#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/ydb_cli/common/common.h>
#include <util/stream/file.h>

#include <unordered_map>
#include <vector>

#include "cert_check.h"

namespace NKikimr {

TCertificateChecker::TCertificateChecker(const TCertificateAuthValues& certificateAuthValues)
    : DynamicNodeAuthorizationParams(GetDynamicNodeAuthorizationParams(certificateAuthValues.ClientCertificateAuthorization))
    , ServerCertificate((certificateAuthValues.ServerCertificateFilePath ? NYdb::NConsoleClient::ReadFromFile(certificateAuthValues.ServerCertificateFilePath, "Can not open server`s certificate") : ""))
    , Domain(certificateAuthValues.Domain)
{ }

TCertificateChecker::TCertificateCheckResult TCertificateChecker::Check(const TString& clientCertificate) const {
    TReadCertificateAsPemResult readCertificateAsPemResult = ReadCertificatesAsPem(clientCertificate);
    if (!readCertificateAsPemResult.Error.empty()) {
        return {.Error = readCertificateAsPemResult.Error, .UserSid = "", .Group = ""};
    }

    const auto& pemCertificates = readCertificateAsPemResult.PemCertificates;
    if (DynamicNodeAuthorizationParams) {
        return CheckClientCertificate(pemCertificates);
    }
    return DefaultCheckClientCertificate(pemCertificates);
}

TCertificateChecker::TReadCertificateAsPemResult TCertificateChecker::ReadCertificatesAsPem(const TString& clientCertificate) const {
    TReadCertificateAsPemResult result;
    TPemCertificates& pemCertificates = result.PemCertificates;
    pemCertificates.ClientCertX509 = X509CertificateReader::ReadCertAsPEM(clientCertificate);
    if (!pemCertificates.ClientCertX509) {
        result.Error = { .Message = "Cannot read client certificate", .Retryable = false };
        return result;
    }

    pemCertificates.ServerCertX509 = X509CertificateReader::ReadCertAsPEM(ServerCertificate);
    if (!pemCertificates.ServerCertX509) {
        result.Error = { .Message = "Cannot read server certificate", .Retryable = false };
        return result;
    }

    return result;
}

TEvTicketParser::TError TCertificateChecker::CheckIssuers(const TPemCertificates& pemCertificates) const {
    const auto& clientIssuerDn = X509CertificateReader::ReadIssuerTerms(pemCertificates.ClientCertX509);
    if (clientIssuerDn.empty()) {
        return { .Message = "Cannot extract issuer dn from client certificate", .Retryable = false };
    }
    const auto& serverIssuerDn = X509CertificateReader::ReadIssuerTerms(pemCertificates.ServerCertX509);
    if (serverIssuerDn.empty()) {
        return { .Message = "Cannot extract issuer dn from server certificate", .Retryable = false };
    }
    if (clientIssuerDn != serverIssuerDn) {
        return { .Message = "Client`s certificate and server`s certificate have different issuers", .Retryable = false };
    }
    return {};
}

TCertificateChecker::TReadClientSubjectResult TCertificateChecker::ReadSubjectFromClientCertificate(const TPemCertificates& pemCertificates) const {
    TReadClientSubjectResult result;
    result.SubjectDn = X509CertificateReader::ReadAllSubjectTerms(pemCertificates.ClientCertX509);
    if (result.SubjectDn.empty()) {
        result.Error = { .Message = "Cannot extract subject from client certificate", .Retryable = false };
        return result;
    }
    return result;
}

TString TCertificateChecker::CreateUserSidFromSubjectDn(const std::vector<std::pair<TString, TString>>& subjectDn) const {
    TStringBuilder userSid;
    for (const auto& [attribute, value] : subjectDn) {
        userSid << attribute << "=" << value << ",";
    }
    userSid.remove(userSid.size() - 1);
    userSid << "@" << Domain;
    return userSid;
}

TEvTicketParser::TError TCertificateChecker::CheckClientSubject(const std::vector<std::pair<TString, TString>>& subjectDn) const {
    std::unordered_map<TString, std::vector<TString>> subjectDescription;
    for (const auto& [attribute, value] : subjectDn) {
        auto& attributeValues = subjectDescription[attribute];
        attributeValues.push_back(value);
    }

    if (!DynamicNodeAuthorizationParams.IsSubjectDescriptionMatched(subjectDescription)) {
        return { .Message = "Client certificate failed verification", .Retryable = false };
    }
    return {};
}

TCertificateChecker::TCertificateCheckResult TCertificateChecker::DefaultCheckClientCertificate(const TPemCertificates& pemCertificates) const {
    TCertificateCheckResult result;
    auto checkIssuersError = CheckIssuers(pemCertificates);
    if (!checkIssuersError.empty()) {
        result.Error = checkIssuersError;
        return result;
    }
    TReadClientSubjectResult readClientSubjectResult = ReadSubjectFromClientCertificate(pemCertificates);
    if (!readClientSubjectResult.Error.empty()) {
        result.Error = readClientSubjectResult.Error;
        return result;
    }
    result.UserSid = CreateUserSidFromSubjectDn(readClientSubjectResult.SubjectDn);
    result.Group = GetDefaultGroup();
    return result;
}

TCertificateChecker::TCertificateCheckResult TCertificateChecker::CheckClientCertificate(const TPemCertificates& pemCertificates) const {
    TCertificateCheckResult result;
    if (DynamicNodeAuthorizationParams.NeedCheckIssuer) {
        auto checkIssuersError = CheckIssuers(pemCertificates);
        if (!checkIssuersError.empty()) {
            result.Error = checkIssuersError;
            return result;
        }
    }
    TReadClientSubjectResult readClientSubjectResult = ReadSubjectFromClientCertificate(pemCertificates);
    if (!readClientSubjectResult.Error.empty()) {
        result.Error = readClientSubjectResult.Error;
        return result;
    }

    auto checkClientSubjectError = CheckClientSubject(readClientSubjectResult.SubjectDn);
    if (!checkClientSubjectError.empty()) {
        result.Error = checkClientSubjectError;
        return result;
    }
    result.UserSid = CreateUserSidFromSubjectDn(readClientSubjectResult.SubjectDn);
    result.Group = (DynamicNodeAuthorizationParams.SidName.empty() ? GetDefaultGroup() : DynamicNodeAuthorizationParams.SidName);
    return result;
}

TString TCertificateChecker::GetDefaultGroup() const {
    return TString(DEFAULT_REGISTER_NODE_CERT_USER) + "@" + Domain;
}

} // namespace NKikimr
