#include "util.h"

#include <util/string/builder.h>
#include <util/string/printf.h>

#include <library/cpp/digest/crc32c/crc32c.h>

namespace NKikimr {

namespace {
TString MaskString(TStringBuf str) {
    TStringBuilder mask;
    if (str.size() >= 16) {
        mask << str.substr(0, 4);
        mask << "****";
        mask << str.substr(str.size() - 4, 4);
    } else {
        mask << "****";
    }
    mask << " (";
    mask << Sprintf("%08X", Crc32c(str.data(), str.size()));
    mask << ")";
    return mask;
}
}

TString MaskTicket(TStringBuf token) {
    return MaskString(token);
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
    return MaskString(certificate.substr(beginCertificateContent, endCertificateContent - beginCertificateContent));
}

TString MaskCertificate(const TString& token) {
    return MaskCertificate(TStringBuf(token));
}

} // namespace NKikimr
