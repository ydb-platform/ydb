#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <random>

namespace NKvVolumeStress {

std::mt19937_64& RandomEngine();
TString MakeVolumePath(const TString& database, const TString& path);
TString ParseHostPort(const TString& endpoint);
TString GeneratePatternData(ui32 size);

} // namespace NKvVolumeStress
