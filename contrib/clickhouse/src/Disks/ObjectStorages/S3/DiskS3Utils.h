#pragma once
#include "clickhouse_config.h"
#include <Core/Types.h>
#include <Common/ObjectStorageKeyGenerator.h>

#if USE_AWS_S3

namespace DBPoco::Util { class AbstractConfiguration; }

namespace DB
{
namespace S3 { struct URI; }

ObjectStorageKeysGeneratorPtr getKeyGenerator(
    const S3::URI & uri,
    const DBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix);

}

#endif
