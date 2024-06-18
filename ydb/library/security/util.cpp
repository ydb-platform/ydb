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

TString PrintCertificateSuffix(TStringBuf certificate) {
    size_t endPos = certificate.rfind("\n-----END");
    if (endPos != TStringBuf::npos && endPos > 0) {
        size_t startPos = certificate.rfind("\n", endPos - 1);
        if (startPos != TStringBuf::npos) {
            size_t len = std::min(endPos - startPos - 1, 16UL);
            return TString(certificate.substr(endPos - len, len));
        }
    }
    return "certificate";
}

TString PrintCertificateSuffix(const TString& certificate) {
    return PrintCertificateSuffix(TStringBuf(certificate));
}

} // namespace NKikimr
