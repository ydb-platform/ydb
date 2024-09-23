#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NBackup::NImpl {

IActor* CreateLocalTableWriter(const TPathId& tablePathId);

}
