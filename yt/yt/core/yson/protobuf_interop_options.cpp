#include "protobuf_interop_options.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TProtobufWriterOptions::TUnknownYsonFieldModeResolver TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode mode)
{
    return [mode] (const NYPath::TYPath& /*path*/) {
        return mode;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
