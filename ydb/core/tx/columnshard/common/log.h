#pragma once

#include <ydb/core/tx/columnshard/defs.h>

using namespace NActors;
using namespace NKikimr::NColumnShard;

#if 1
#define TEMPLOG(x) LOG_S_CRIT(x)
#else
#define TEMPLOG(x)
#endif
