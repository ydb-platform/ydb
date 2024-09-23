#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessDynamicTableWriter(NYPath::TYPath path, NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
