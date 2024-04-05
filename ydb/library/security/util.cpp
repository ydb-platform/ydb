#include "util.h"

#include <util/string/builder.h>
#include <util/string/printf.h>

#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {

TString MaskTicket(TStringBuf token) {
    TStringBuilder mask;
    if (token.size() >= 16) {
        mask << token.substr(0, 4);
        mask << "****";
        mask << token.substr(token.size() - 4, 4);
    } else {
        mask << "****";
    }
    mask << " (";
    mask << Sprintf("%08X", Crc32c(token.data(), token.size()));
    mask << ")";
    return mask;
}

TString MaskTicket(const TString& token) {
    return MaskTicket(TStringBuf(token));
}

TString MaskCertificate(TStringBuf certificate) {
    size_t beginCertificateContent = 0;
    if (size_t pos = certificate.find('\n'); pos != TStringBuf::npos) {
        beginCertificateContent = pos + 1;
    }
    size_t endCertificateContent = beginCertificateContent;
    if (size_t pos = certificate.rfind("\n-----END"); pos != TStringBuf::npos) {
        endCertificateContent = pos;
    }
    return MaskTicket(certificate.substr(beginCertificateContent, endCertificateContent - beginCertificateContent));
}

TString MaskCertificate(const TString& token) {
    return MaskCertificate(TStringBuf(token));
}

} // namespace NKikimr
