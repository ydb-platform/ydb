#pragma once

#include <util/generic/string.h>

namespace NKikimr::NUdfStore {

TString GetUdfStorePrefix();
TString GetWasmSourceTablePath();
TString GetLibrarySourceTablePath();
TString GetArtifactTablePath(const TString& cpuSpec);
TString NormalizeCpuSpec(TStringBuf triple, TStringBuf cpu);

} // namespace NKikimr::NUdfStore
