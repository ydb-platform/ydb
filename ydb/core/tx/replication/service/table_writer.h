#pragma once

#include <ydb/core/base/defs.h>

namespace NKikimrReplication { class TLocalTableWriterSettings; }

namespace NKikimr {
    struct TPathId;
}

namespace NKikimr::NReplication::NService {

enum class EWriteMode {
    Simple,
    Consistent,
};

IActor* CreateLocalTableWriter(const TString& database, const TPathId& tablePathId, EWriteMode mode = EWriteMode::Simple,
    const NKikimrReplication::TLocalTableWriterSettings* settings = nullptr);

}
