#include "validators.h"
#include <ydb/core/protos/config.pb.h>
#include <util/generic/string.h>
#include <vector>

namespace NKikimr::NConfig {

EValidationResult ValidateClientCertificateAuthorization(
    const NKikimrConfig::TAppConfig& config,
    std::vector<TString>& msg
) {
    if (!config.HasClientCertificateAuthorization()) {
        return EValidationResult::Ok;
    }

    const auto& clientCertificateAuthorization = config.GetClientCertificateAuthorization();
    if (clientCertificateAuthorization.GetClientCertificateRequired()
            && !clientCertificateAuthorization.GetRequestClientCertificate()) {
        msg.push_back("RequestClientCertificate is disabled, but ClientCertificateRequired is enabled");
        return EValidationResult::Error;
    }

    return EValidationResult::Ok;
}

} // NKikimr::NConfig
