#pragma once

#include <util/generic/string.h>
#include <map>

namespace NYql {
namespace NCommon {

TMaybe<TString> SqlToSExpr(const TString& query);

TString GetSerializedResultType(const TString& program);

bool ParseCounterName(TString* prefix, std::map<TString, TString>* labels, TString* name, const TString& counterName);

} // namespace NCommon
} // namespace NYql
