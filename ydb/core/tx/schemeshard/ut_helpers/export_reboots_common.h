#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSchemeShardUT_Private {

class TTestWithReboots;

namespace NExportReboots {

void Run(const TVector<TString>& tables, const TString& request, TTestWithReboots& t);
void Cancel(const TVector<TString>& tables, const TString& request, TTestWithReboots& t);
void Forget(const TVector<TString>& tables, const TString& request, TTestWithReboots& t);

} // NExportReboots
} // NSchemeShardUT_Private
