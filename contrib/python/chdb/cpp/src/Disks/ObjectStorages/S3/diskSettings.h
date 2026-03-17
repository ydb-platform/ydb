#pragma once

#include "clickhouse_config.h"

#if USE_AWS_S3

#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <Interpreters/Context_fwd.h>

#include <IO/S3/Client.h>

namespace DB_CHDB
{

struct S3ObjectStorageSettings;

std::unique_ptr<S3ObjectStorageSettings> getSettings(
    const CHDBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    const std::string & endpoint,
    bool validate_settings);

std::unique_ptr<S3::Client> getClient(
    const std::string & endpoint,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3);

std::unique_ptr<S3::Client> getClient(
    const S3::URI & url_,
    const S3ObjectStorageSettings & settings,
    ContextPtr context,
    bool for_disk_s3);

}

#endif
