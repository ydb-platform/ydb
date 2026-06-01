#pragma once

#include <ydb/core/protos/config.pb.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>
#include "cert_auth_processor.h"

namespace NKikimr {
struct TCertificateAuthValues {
    NKikimrConfig::TClientCertificateAuthorization ClientCertificateAuthorization;
    TString ServerCertificateFilePath;
    TString Domain;
};

enum class ECertificateFormat {
    PEM,
    DER
};

std::vector<TCertificateAuthorizationParams> GetCertificateAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization& clientCertificateAuth);
NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm MakeSubjectTerm(const TString& name, const TVector<TString>& values, const TVector<TString>& suffixes = {});

std::string GetCertificateFingerprint(const std::string& certificate, ECertificateFormat certFormat = ECertificateFormat::PEM);
std::string GetCertificatePublicKey(const std::string& certificate, ECertificateFormat certFormat = ECertificateFormat::PEM);

} //namespace NKikimr
