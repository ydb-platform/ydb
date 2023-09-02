#pragma once

#include "public.h"

#include <yt/yt/library/query/base/builtin_function_registry.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/functions_builder.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)

struct TFunctionProfilerMap
    : public TRefCounted
    , public std::unordered_map<TString, IFunctionCodegenPtr>
{
    const IFunctionCodegenPtr& GetFunction(const TString& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TFunctionProfilerMap)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFunctionProfilerMap)

struct TAggregateProfilerMap
    : public TRefCounted
    , public std::unordered_map<TString, IAggregateCodegenPtr>
{
    const IAggregateCodegenPtr& GetAggregate(const TString& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TAggregateProfilerMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
