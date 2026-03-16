#pragma once
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>

namespace CHDBPoco::Util { class AbstractConfiguration; }

namespace DB_CHDB
{
void loadDiskLocalConfig(
    const String & name,
    const CHDBPoco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context,
    String & path,
    UInt64 & keep_free_space_bytes);
}
