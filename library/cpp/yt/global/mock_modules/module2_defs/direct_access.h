#pragma once

#include <library/cpp/yt/misc/guid.h>

#include <array>
#include <cstdint>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TGuid GetTestVariable2();

void SetTestVariable2(TGuid val);

std::array<int, 4> GetTestVariable3();

void SetTestVariable3(std::array<int, 4> val);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
