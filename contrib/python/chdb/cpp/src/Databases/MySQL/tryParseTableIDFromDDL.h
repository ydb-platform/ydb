#pragma once

#include <base/types.h>
#include <Storages/IStorage.h>

namespace DB_CHDB
{

StorageID tryParseTableIDFromDDL(const String & query, const String & default_database_name);

}
