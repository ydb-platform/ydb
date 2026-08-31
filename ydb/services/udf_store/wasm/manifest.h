#pragma once

#include "types.h"

namespace NKikimr::NUdfStore::NWasm {

TWasmManifest ParseManifest(TStringBuf manifestJson);

} // namespace NKikimr::NUdfStore::NWasm
