#pragma once

#include <yt/cpp/mapreduce/interface/init.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

enum class EInitStatus : int
{
    NotInitialized,
    JoblessInitialization,
    FullInitialization,
};

EInitStatus& GetInitStatus();

void EnsureInitialized();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
