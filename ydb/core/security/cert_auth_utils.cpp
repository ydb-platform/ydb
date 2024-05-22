#include "cert_auth_utils.h"

namespace NKikimr {

TDynamicNodeAuthorizationParams GetDynamicNodeAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization &clientSertificateAuth) {
    TDynamicNodeAuthorizationParams certAuthConf;
    if (!clientSertificateAuth.HasDynamicNodeAuthorization()) {
        return certAuthConf;
    }

    const auto& dynNodeAuth = clientSertificateAuth.GetDynamicNodeAuthorization();
    TDynamicNodeAuthorizationParams::TDistinguishedName distinguishedName;
    for (const auto& term: dynNodeAuth.GetSubjectTerms()) {
        auto name = TDynamicNodeAuthorizationParams::TRelativeDistinguishedName(term.GetShortName());
        for (const auto& value: term.GetValues()) {
            name.AddValue(value);
        }
        for (const auto& suffix: term.GetSuffixes()) {
            name.AddSuffix(suffix);
        }
        distinguishedName.AddRelativeDistinguishedName(std::move(name));
    }
    if (!distinguishedName.RelativeDistinguishedNames.empty()) {
        certAuthConf.AddCertSubjectDescription(distinguishedName);
    }
    certAuthConf.CanCheckNodeByAttributeCN = dynNodeAuth.GetCanCheckNodeHostByCN();
    return certAuthConf;
}

}  //namespace NKikimr
