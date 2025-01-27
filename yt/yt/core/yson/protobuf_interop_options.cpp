#include "protobuf_interop_options.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TProtobufWriterOptions::TUnknownYsonFieldModeResolver TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(EUnknownYsonFieldsMode mode)
{
    return [mode] (const NYPath::TYPath& /*path*/) {
        return mode;
    };
}

TProtobufWriterOptions TProtobufWriterOptions::CreateChildOptions(
    const NYPath::TYPath& path) const
{
    auto options = *this;
    options.UnknownYsonFieldModeResolver = [path, parentResolver = UnknownYsonFieldModeResolver] (
        const NYPath::TYPath& subpath)
    {
        return parentResolver(path + subpath);
    };

    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
