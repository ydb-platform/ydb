#include "operation_idempotency.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>

#include <array>

namespace NKikimr::NSchemeShard {
namespace {

using TKqpOperation = NKqpProto::TKqpSchemeOperation;
using TModifyScheme = NKikimrSchemeOp::TModifyScheme;

struct TOperationIdempotencySupport {
    NKikimrSchemeOp::EOperationType OperationType;
    TStringBuf SqlWriteMode;
    TKqpOperation::OperationCase KqpOperationCase;
    const TModifyScheme& (TKqpOperation::*GetPayload)() const;
};

// Single capability list for SQL validation, KQP execution, and SchemeShard
// admission. Unlisted operations do not support UID-based idempotency.
const std::array SupportedOperations = {
    TOperationIdempotencySupport{NKikimrSchemeOp::ESchemeOpBackupBackupCollection,
        "backup", TKqpOperation::kBackup, &TKqpOperation::GetBackup},
    TOperationIdempotencySupport{NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection,
        "backupIncremental", TKqpOperation::kBackupIncremental, &TKqpOperation::GetBackupIncremental},
    TOperationIdempotencySupport{NKikimrSchemeOp::ESchemeOpRestoreBackupCollection,
        "restore", TKqpOperation::kRestore, &TKqpOperation::GetRestore},
};

} // namespace

bool SupportsOperationIdempotency(NKikimrSchemeOp::EOperationType operationType) {
    for (const auto& supported : SupportedOperations) {
        if (supported.OperationType == operationType) {
            return true;
        }
    }
    return false;
}

bool SupportsSqlOperationIdempotency(TStringBuf writeMode) {
    for (const auto& supported : SupportedOperations) {
        if (supported.SqlWriteMode == writeMode) {
            return true;
        }
    }
    return false;
}

const TModifyScheme* GetSchemeOperationForIdempotency(const TKqpOperation& operation) {
    // Object operations follow a separate executor path.
    if (!operation.GetObjectType().empty()) {
        return nullptr;
    }
    for (const auto& supported : SupportedOperations) {
        if (supported.KqpOperationCase == operation.GetOperationCase()) {
            const auto& payload = (operation.*supported.GetPayload)();
            return payload.GetOperationType() == supported.OperationType ? &payload : nullptr;
        }
    }
    return nullptr;
}

} // namespace NKikimr::NSchemeShard
