#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, TableClientLogger, "TableClient");

////////////////////////////////////////////////////////////////////////////////

struct TTypeV3Info
{
    ESimpleLogicalValueType V1Type;
    EValueType WireType;
    bool Required;
    bool IsPureV1Type;
};

TTypeV3Info GetTypeV3Info(const TLogicalTypePtr& logicalType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
