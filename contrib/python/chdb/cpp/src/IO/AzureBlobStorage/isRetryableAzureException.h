#pragma once
#include "clickhouse_config.h"

#if USE_AZURE_BLOB_STORAGE
#error #include <azure/core/http/http.hpp>

namespace DB_CHDB
{

bool isRetryableAzureException(const Azure::Core::RequestFailedException & e);

}

#endif
