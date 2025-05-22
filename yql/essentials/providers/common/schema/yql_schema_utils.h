#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
namespace NCommon {

TVector<TString> ExtractColumnOrderFromYsonStructType(const NYT::TNode& node);
bool EqualsYsonTypesIgnoreStructOrder(const NYT::TNode& left, const NYT::TNode& right);

} // namespace NCommon
} // namespace NYql
