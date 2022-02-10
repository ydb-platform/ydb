#pragma once

#include <util/generic/string.h>

namespace NHtml {
    TString EscapeAttributeValue(const TString& value);
    TString EscapeText(const TString& value);

}
