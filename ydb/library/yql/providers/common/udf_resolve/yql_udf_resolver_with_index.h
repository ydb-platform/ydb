#pragma once

#include <ydb/library/yql/core/yql_udf_index.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>

namespace NYql {
namespace NCommon {

IUdfResolver::TPtr CreateUdfResolverWithIndex(TUdfIndex::TPtr udfIndex, IUdfResolver::TPtr fallback, TFileStoragePtr fileStorage);

} // namespace NCommon
} // namespace NYql
