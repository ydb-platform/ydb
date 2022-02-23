#pragma once
#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NYql {

TMaybe<TString> LookupFunctionSignature(const TString& name);

}
