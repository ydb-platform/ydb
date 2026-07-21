#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP::NSupportLinks {

TVector<TString> ExtractUrlTemplateParameters(TStringBuf urlTemplate);
void ValidateUrlTemplateSyntax(TStringBuf urlTemplate);

} // namespace NMVP::NSupportLinks
