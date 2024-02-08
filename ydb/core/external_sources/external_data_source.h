#pragma once

#include "external_source.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/set.h>

namespace NKikimr::NExternalSource {

IExternalSource::TPtr CreateExternalDataSource(const TString& name, const TVector<TString>& authMethods, const TSet<TString>& availableProperties, const std::vector<TRegExMatch>& hostnamePatterns);

}
