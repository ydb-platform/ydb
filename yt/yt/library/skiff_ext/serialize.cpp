#include "serialize.h"

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

void Serialize(EWireType wireType, NYT::NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(::ToString(wireType));
}

void Deserialize(EWireType& wireType, NYT::NYTree::INodePtr node)
{
    if (node->GetType() != NYT::NYTree::ENodeType::String) {
        THROW_ERROR_EXCEPTION("Cannot deserialize Skiff wire type from %Qlv node, expected %Qlv",
            node->GetType(),
            NYT::NYTree::ENodeType::String);
    }
    wireType = ::FromString<EWireType>(node->GetValue<TString>());
}

void Deserialize(EWireType& wireType, NYT::NYson::TYsonPullParserCursor* cursor)
{
    NYT::NYson::MaybeSkipAttributes(cursor);
    NYT::NYson::EnsureYsonToken("Skiff wire type", *cursor, NYT::NYson::EYsonItemType::StringValue);
    wireType = ::FromString<EWireType>((*cursor)->UncheckedAsString());
    cursor->Next();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
