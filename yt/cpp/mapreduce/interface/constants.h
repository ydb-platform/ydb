#pragma once


#include <util/system/defaults.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////


// Maximum number of input tables for operation.
// If greater number of input tables are provided behaviour is undefined
// (it might work ok or it might fail or it might work very slowly).
constexpr size_t MaxInputTableCount = 1000;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
