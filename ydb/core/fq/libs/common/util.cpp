#include "util.h"

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NFq {

TString EscapeString(const TString& value,
                     const TString& enclosingSeq,
                     const TString& replaceWith) {
    auto escapedValue = value;
    SubstGlobal(escapedValue, enclosingSeq, replaceWith);
    return escapedValue;
}

TString EscapeString(const TString& value, char enclosingChar) {
    auto escapedValue = value;
    SubstGlobal(escapedValue,
                TString{enclosingChar},
                TStringBuilder{} << '\\' << enclosingChar);
    return escapedValue;
}

TString EncloseAndEscapeString(const TString& value, char enclosingChar) {
    return TStringBuilder{} << enclosingChar << EscapeString(value, enclosingChar)
                            << enclosingChar;
}

TString EncloseAndEscapeString(const TString& value,
                               const TString& enclosingSeq,
                               const TString& replaceWith) {
    return TStringBuilder{} << enclosingSeq
                            << EscapeString(value, enclosingSeq, replaceWith)
                            << enclosingSeq;
}

} // namespace NFq
