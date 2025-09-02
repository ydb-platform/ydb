#pragma once

#include "ut_backup_restore_common.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NActors {
    class TTestActorRuntime;
}

namespace NSchemeShardUT_Private {

class TTestWithReboots;

namespace NExportReboots {

void Run(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Cancel(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void Forget(const TVector<TTypedScheme>& schemeObjects, const TString& request, TTestWithReboots& t);
void CreateSchemeObjects(TTestWithReboots& t, NActors::TTestActorRuntime& runtime, const TVector<TTypedScheme>& schemeObjects);

} // NExportReboots
} // NSchemeShardUT_Private
