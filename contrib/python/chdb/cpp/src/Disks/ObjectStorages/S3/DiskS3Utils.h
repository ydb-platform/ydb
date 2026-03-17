#pragma once
#include "clickhouse_config.h"
#include <Core/Types.h>
#include <Common/ObjectStorageKeyGenerator.h>

#if USE_AWS_S3

namespace CHDBPoco::Util { class AbstractConfiguration; }

namespace DB_CHDB
{
namespace S3 { struct URI; }

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    const S3::URI & uri,
    const CHDBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix);

class S3ObjectStorage;
bool checkBatchRemove(S3ObjectStorage & storage);

}

#endif
