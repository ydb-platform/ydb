#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TProtobufParserType)
DECLARE_REFCOUNTED_CLASS(TProtobufWriterType)

class TProtobufParserFieldDescription;
class TProtobufWriterFieldDescription;

DECLARE_REFCOUNTED_CLASS(TProtobufParserFormatDescription)
DECLARE_REFCOUNTED_CLASS(TProtobufWriterFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
