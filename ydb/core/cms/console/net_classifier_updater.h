#pragma once

#include "defs.h"
#include "console.h"

namespace NKikimr::NNetClassifierUpdater {

IActor* MakeNetClassifierUpdaterActor(TActorId localConsole);

TString UnpackNetData(const TString& packedNetData);

} // namespace NKikimr::NNetClassifierUpdater
