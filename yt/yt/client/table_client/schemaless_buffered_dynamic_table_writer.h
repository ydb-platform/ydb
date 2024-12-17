#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessBufferedDynamicTableWriter(
    NYPath::TYPath path,
    NApi::IClientPtr client,
    TSchemalessBufferedDynamicTableWriterConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
