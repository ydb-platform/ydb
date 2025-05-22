#pragma once

#include <yql/essentials/core/yql_udf_index.h>
#include <yql/essentials/core/file_storage/file_storage.h>

namespace NYql {
namespace NCommon {

IUdfResolver::TPtr CreateUdfResolverWithIndex(TUdfIndex::TPtr udfIndex, IUdfResolver::TPtr fallback, TFileStoragePtr fileStorage);

} // namespace NCommon
} // namespace NYql
