#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/ydb_cli/common/common.h>
#include <util/stream/file.h>

#include <unordered_map>
#include <vector>

#include "cert_check.h"

namespace NKikimr {

namespace {
TString ReadFromFile(const TString& filePath, const TString& fileName, bool allowEmpty = false) {
    TString fpCopy = filePath;
    return NYdb::NConsoleClient::ReadFromFile(fpCopy, fileName, allowEmpty);
}
} // namespace

TCertificateChecker::TCertificateChecker(const TCertificateAuthValues& certificateAuthValues)
    : CertificateAuthorizationParams(GetCertificateAuthorizationParams(certificateAuthValues.ClientCertificateAuthorization))
    , ServerCertificate((certificateAuthValues.ServerCertificateFilePath ? ReadFromFile(certificateAuthValues.ServerCertificateFilePath, "Can not open server`s certificate") : ""))
    , Domain(certificateAuthValues.Domain)
    , DefaultGroup(certificateAuthValues.ClientCertificateAuthorization.GetDefaultGroup())
{ }

TCertificateChecker::TCertificateCheckResult TCertificateChecker::Check(const TString& clientCertificate) const {
    // ToDo First of all need verify client`s certificate by server CA

    TReadCertificateAsPemResult readCertificateAsPemResult = ReadCertificatesAsPem(clientCertificate);
    if (!readCertificateAsPemResult.Error.empty()) {
        return {.Error = readCertificateAsPemResult.Error, .UserSid = "", .Groups = {""}};
    }

    const auto& pemCertificates = readCertificateAsPemResult.PemCertificates;
    if (CertificateAuthorizationParams.empty()) {
        return DefaultCheckClientCertificate(pemCertificates);
    }
    return CheckClientCertificate(pemCertificates);
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
    result.SubjectDns = X509CertificateReader::ReadSubjectDns(pemCertificates.ClientCertX509, result.SubjectDn);
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

TEvTicketParser::TError TCertificateChecker::CheckClientSubject(const TReadClientSubjectResult& subjectInfo, const TCertificateAuthorizationParams& authParams) const {
    std::unordered_map<TString, std::vector<TString>> subjectDescription;
    for (const auto& [attribute, value] : subjectInfo.SubjectDn) {
        auto& attributeValues = subjectDescription[attribute];
        attributeValues.push_back(value);
    }

    if (!authParams.CheckSubject(subjectDescription, subjectInfo.SubjectDns)) {
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
    result.Groups = {DefaultGroup};
    return result;
}

TCertificateChecker::TCertificateCheckResult TCertificateChecker::CheckClientCertificate(const TPemCertificates& pemCertificates) const {
    TCertificateCheckResult result;
    auto checkIssuersError = CheckIssuers(pemCertificates);
    TReadClientSubjectResult readClientSubjectResult = ReadSubjectFromClientCertificate(pemCertificates);
    if (!readClientSubjectResult.Error.empty()) {
        result.Error = readClientSubjectResult.Error;
        return result;
    }
    std::unordered_set<TString> groups;
    for (const auto& authParams : CertificateAuthorizationParams) {
        if (authParams.RequireSameIssuer && !checkIssuersError.empty()) {
            continue;
        }

        auto checkClientSubjectError = CheckClientSubject(readClientSubjectResult, authParams);
        if (!checkClientSubjectError.empty()) {
            continue;
        }
        if (authParams.Groups.empty()) {
            groups.insert(DefaultGroup);
        } else {
            groups.insert(authParams.Groups.cbegin(), authParams.Groups.cend());
        }
    }
    if (groups.empty()) {
        result.Error = { .Message = "Client certificate failed verification", .Retryable = false };
        return result;
    }
    result.UserSid = CreateUserSidFromSubjectDn(readClientSubjectResult.SubjectDn);
    result.Groups.assign(groups.cbegin(), groups.cend());
    return result;
}

} // namespace NKikimr
