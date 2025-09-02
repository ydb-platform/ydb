#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

/*
These parse functions can understand nan, inf, -inf case-insensitively
They do not parse empty string to zero
*/

float FloatFromString(TStringBuf buf);
double DoubleFromString(TStringBuf buf);

bool TryFloatFromString(TStringBuf buf, float& value);
bool TryDoubleFromString(TStringBuf buf, double& value);

}
