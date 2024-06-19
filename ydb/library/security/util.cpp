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

} // namespace NKikimr
