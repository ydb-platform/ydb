#pragma once

#include <util/generic/string.h>

namespace NKikimr::NUdfStore {

TString GetUdfStorePrefix();
TString GetWasmSourceTablePath();
TString GetWasmSourceChunksTablePath();
TString GetLibrarySourceTablePath();
TString GetLibrarySourceChunksTablePath();
TString GetArtifactTablePath(const TString& cpuSpec);
TString GetArtifactChunksTablePath(const TString& cpuSpec);
TString NormalizeCpuSpec(TStringBuf triple, TStringBuf cpu);

} // namespace NKikimr::NUdfStore
