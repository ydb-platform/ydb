#pragma once

#include "public.h"
#include "table_reader.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

/// @brief Asynchronous byte stream of table data encoded in a specified format.
struct IFormattedTableReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
    //! Returns timing statistics accumulated so far; see #ITableReader::GetTimingStatistics.
    virtual TTableReaderTimingStatistics GetTimingStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFormattedTableReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
