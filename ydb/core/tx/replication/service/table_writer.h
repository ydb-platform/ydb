#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NReplication::NService {

enum class EWriteMode {
    Simple,
    Consistent,
};

IActor* CreateLocalTableWriter(const TPathId& tablePathId, EWriteMode mode = EWriteMode::Simple);

}
