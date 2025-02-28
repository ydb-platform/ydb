#pragma once

#include <ydb/library/yql/dq/actors/dq.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <util/generic/string.h>
#include <map>

namespace NYql {
namespace NCommon {

struct TResultFormatSettings {
    TString ResultType;
    TVector<TString> Columns;
    TMaybe<ui64> SizeLimit;
    TMaybe<ui64> RowsLimit;
};

TString GetSerializedTypeAnnotation(const NYql::TTypeAnnotationNode* typeAnn);
TString GetSerializedResultType(const TString& program);

bool ParseCounterName(TString* prefix, std::map<TString, TString>* labels, TString* name, const TString& counterName);

bool IsRetriable(NYql::NDqProto::StatusIds::StatusCode statusCode);
bool IsRetriable(const NDq::TEvDq::TEvAbortExecution::TPtr& ev);
bool NeedFallback(NYql::NDqProto::StatusIds::StatusCode statusCode);
bool NeedFallback(const NDq::TEvDq::TEvAbortExecution::TPtr& ev);
} // namespace NCommon
} // namespace NYql
