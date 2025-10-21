#pragma once

#include "yson_schema_options.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NYTree::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void WriteSchema(const T& value, NYson::IYsonConsumer* consumer, const TYsonStructWriteSchemaOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NPrivate

#define YSON_SCHEMA_INL_H_
#include "yson_schema-inl.h"
#undef YSON_SCHEMA_INL_H_
