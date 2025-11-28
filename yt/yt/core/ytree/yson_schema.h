#pragma once

#include "yson_schema_options.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void WriteSchema(NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YSON_SCHEMA_INL_H_
#include "yson_schema-inl.h"
#undef YSON_SCHEMA_INL_H_
