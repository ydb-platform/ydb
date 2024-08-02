#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NBackup::NImpl {

enum class EWriterType {
    Backup,
    Restore,
};

IActor* CreateLocalTableWriter(const TPathId& tablePathId, EWriterType type = EWriterType::Backup);

}
