#include "protobuf_interop_options.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TProtobufWriterOptions::TUnknownYsonFieldModeResolver TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode mode)
{
    return [mode] (NYPath::TYPathBuf /*path*/) {
        return mode;
    };
}

TProtobufWriterOptions TProtobufWriterOptions::CreateChildOptions(NYPath::TYPathBuf path) const
{
    auto options = *this;
    options.UnknownYsonFieldModeResolver =
        [path = NYPath::TYPath(path), parentResolver = UnknownYsonFieldModeResolver] (NYPath::TYPathBuf subpath)
        {
            return parentResolver(path + subpath);
        };

    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
