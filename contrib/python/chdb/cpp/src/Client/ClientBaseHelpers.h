#pragma once

#include <Core/Types.h>
#include "clickhouse_config.h"

#if USE_REPLXX
#   include <Client/ReplxxLineReader.h>
#endif


namespace DB_CHDB
{

/// Should we celebrate a bit?
bool isNewYearMode();

bool isChineseNewYearMode(const String & local_tz);

#if USE_REPLXX
void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors);
#endif

}
