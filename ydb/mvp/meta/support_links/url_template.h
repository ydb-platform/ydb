#pragma once

#include <util/generic/string.h>

namespace NMVP::NSupportLinks {

bool HasUrlTemplateExpressions(TStringBuf urlTemplate);
bool HasUrlTemplateParameter(TStringBuf urlTemplate, TStringBuf parameterName);
void ValidateUrlTemplateSyntax(TStringBuf urlTemplate);

} // namespace NMVP::NSupportLinks
