#include "yson_helpers.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/yson/string.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TString CanonizeYson(TStringBuf input)
{
    auto node = ConvertToNode(TYsonString(input));
    auto binaryYson = ConvertToYsonString(node);

    TStringStream out;
    {
        TYsonWriter writer(&out, NYson::EYsonFormat::Pretty);
        ParseYsonStringBuffer(binaryYson.AsStringBuf(), EYsonType::Node, &writer);
    }
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
