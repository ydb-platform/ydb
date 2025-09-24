#pragma once

#include "ut_backup_restore_common.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSchemeShardUT_Private {

class TTestWithReboots;

namespace NExportReboots {

void Run(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Cancel(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Forget(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);

} // NExportReboots
} // NSchemeShardUT_Private
