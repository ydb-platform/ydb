#pragma once

#include "public.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

/// @brief Asynchronous byte stream of table data encoded in a specified format.
struct IFormattedTableReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{ };

DEFINE_REFCOUNTED_TYPE(IFormattedTableReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
