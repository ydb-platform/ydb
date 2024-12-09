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

namespace {

// Ticket is like ne1<token>.<signature>
// Finds pos of '.'
size_t FindNebiusTokenSignaturePos(const TString& token) {
    if (!token.StartsWith("ne1")) {
        return TString::npos;
    }
    size_t pos = token.find('.');
    if (pos == TString::npos) {
        return pos;
    }
    if (pos < token.size() - 1) { // '.' is not the last symbol
        return pos;
    }
    return TString::npos;
}

} // namespace

TString SanitizeNebiusTicket(const TString& token) {
    const size_t signaturePos = FindNebiusTokenSignaturePos(token);
    if (signaturePos == TString::npos) {
        return MaskTicket(token);
    }
    return TStringBuilder() << TStringBuf(token).SubString(0, signaturePos) << ".**"; // <token>.**
}

TString MaskNebiusTicket(const TString& token) {
    const size_t signaturePos = FindNebiusTokenSignaturePos(token);
    if (signaturePos == TString::npos) {
        return MaskTicket(token);
    }
    return MaskTicket(TStringBuf(token).SubString(0, signaturePos));
}

} // namespace NKikimr
