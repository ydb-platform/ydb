#pragma once

#include <util/generic/string.h>

namespace NKikimr::NUdfStore {

TString DetectLocalCpuSpec(const TString& overrideSpec = {});

} // namespace NKikimr::NUdfStore
