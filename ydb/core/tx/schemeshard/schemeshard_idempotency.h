#pragma once

#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <utility>

namespace NKikimr::NSchemeShard {

// A UID belongs to an operation type on this SchemeShard tablet.
using TBackupOperationUidKey = std::pair<ui32, TString>;

struct TBackupOperationReplay {
    ui64 OperationId = 0;
    TString OriginalDdl;
    TString UserSID;
};

} // namespace NKikimr::NSchemeShard
