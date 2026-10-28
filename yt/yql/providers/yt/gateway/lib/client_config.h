#pragma once

#include <yt/cpp/mapreduce/interface/config.h>

namespace NYql {

struct TYtSettings;

NYT::TConfigPtr CreateYtClientConfig(const TYtSettings& settings);

} // namespace NYql
