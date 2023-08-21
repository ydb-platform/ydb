#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/skiff/public.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

void Serialize(EWireType wireType, NYT::NYson::IYsonConsumer* consumer);
void Deserialize(EWireType& wireType, NYT::NYTree::INodePtr node);
void Deserialize(EWireType& wireType, NYT::NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff
