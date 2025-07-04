#pragma once
#include "defs.h"
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NSQS {

const NKikimrConfig::TSqsConfig& Cfg();
ui32 GetLeadersDescriberUpdateTimeMs();

} // namespace NKikimr::NSQS
