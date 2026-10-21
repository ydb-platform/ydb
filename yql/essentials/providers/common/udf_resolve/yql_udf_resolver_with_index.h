#pragma once

#include <yql/essentials/core/yql_udf_index.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/core/url_preprocessing/interface/url_preprocessing.h>

namespace NYql::NCommon {

IUdfResolver::TPtr CreateUdfResolverWithIndex(
    TUdfIndex::TPtr udfIndex,
    IUdfResolver::TPtr fallback,
    TFileStoragePtr fileStorage,
    IUrlPreprocessing::TPtr urlPreprocessing = {},
    TTokenResolver tokenResolver = {});

} // namespace NYql::NCommon
