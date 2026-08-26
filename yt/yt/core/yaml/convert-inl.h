#ifndef YAML_CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include convert.h"
#include "convert.h"
#endif

#include "config.h"
#include "parser.h"

#include <yt/yt/core/yson/yson_builder.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/stream/mem.h>

namespace NYT::NYaml {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ConvertFromYaml(TStringBuf yaml)
{
    TMemoryInput input(yaml);
    auto config = New<TYamlFormatConfig>();
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Binary, NYson::EYsonType::Node);
    ParseYaml(&input, builder.GetConsumer(), config, NYson::EYsonType::Node);
    return NYTree::ConvertTo<T>(NYTree::ConvertToNode(builder.Flush()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYaml
