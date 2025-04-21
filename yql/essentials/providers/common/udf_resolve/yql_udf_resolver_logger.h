#pragma once

#include <yql/essentials/core/yql_udf_index.h>
#include <yql/essentials/core/file_storage/file_storage.h>

namespace NYql::NCommon {

IUdfResolver::TPtr CreateUdfResolverDecoratorWithLogger(IUdfResolver::TPtr underlying, const TString& path, const TString& sessionId);

} // namespace NYql::NCommon
