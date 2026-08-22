#pragma once

#include <library/cpp/yson/node/node.h>

namespace NYql::NCommon {

NYT::TNode ParseSkiffTypeFromYson(const NYT::TNode& node, ui64 nativeYTTypesFlags);

} // namespace NYql::NCommon
