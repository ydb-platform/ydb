#pragma once
#include "defs.h"

#include <ydb/core/engine/mkql_engine_flat.h>

namespace NKikimr {
namespace NDataShard {

bool HasKeyConflict(const NMiniKQL::IEngineFlat::TValidationInfo& infoA,
                    const NMiniKQL::IEngineFlat::TValidationInfo& infoB);

} // namespace NDataShard
} // namespace NKikimr
