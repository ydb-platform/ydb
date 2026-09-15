#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <array>
#include <optional>

namespace NKikimr::NSchemeShard {
namespace {

using TKqpOperation = NKqpProto::TKqpSchemeOperation;
using TModifyScheme = NKikimrSchemeOp::TModifyScheme;

struct TSchemeOperationUidSupport {
    NKikimrSchemeOp::EOperationType OperationType;
    TStringBuf SqlWriteMode;
    TKqpOperation::OperationCase KqpOperationCase;
    const TModifyScheme& (TKqpOperation::*GetPayload)() const;
};

struct TOperationUidSupport {
    Ydb::TOperationId::EKind Kind;
    std::optional<TSchemeOperationUidSupport> SchemeOperation;
};

// All SchemeShard UID-capable operation kinds. A missing scheme mapping means
// the operation uses its existing RPC admission path and has no SQL UID support.
// Duplicate handling remains specific to the operation's existing protocol.
const std::array SupportedOperations = {
    TOperationUidSupport{Ydb::TOperationId::EXPORT, std::nullopt},
    TOperationUidSupport{Ydb::TOperationId::IMPORT, std::nullopt},
    TOperationUidSupport{Ydb::TOperationId::BUILD_INDEX, std::nullopt},
    TOperationUidSupport{Ydb::TOperationId::SET_NOT_NULL, std::nullopt},
    TOperationUidSupport{Ydb::TOperationId::FULL_BACKUP,
        TSchemeOperationUidSupport{NKikimrSchemeOp::ESchemeOpBackupBackupCollection,
            "backup", TKqpOperation::kBackup, &TKqpOperation::GetBackup}},
    TOperationUidSupport{Ydb::TOperationId::INCREMENTAL_BACKUP,
        TSchemeOperationUidSupport{NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection,
            "backupIncremental", TKqpOperation::kBackupIncremental, &TKqpOperation::GetBackupIncremental}},
    TOperationUidSupport{Ydb::TOperationId::RESTORE,
        TSchemeOperationUidSupport{NKikimrSchemeOp::ESchemeOpRestoreBackupCollection,
            "restore", TKqpOperation::kRestore, &TKqpOperation::GetRestore}},
};

} // namespace

bool SupportsOperationUid(Ydb::TOperationId::EKind kind) {
    for (const auto& supported : SupportedOperations) {
        if (supported.Kind == kind) {
            return true;
        }
    }
    return false;
}

bool SupportsSqlOperationIdempotency(Ydb::TOperationId::EKind kind) {
    for (const auto& supported : SupportedOperations) {
        if (supported.Kind == kind) {
            return supported.SchemeOperation.has_value();
        }
    }
    return false;
}

TMaybe<Ydb::TOperationId_EKind> GetOperationUidKind(NKikimrSchemeOp::EOperationType operationType) {
    for (const auto& supported : SupportedOperations) {
        if (supported.SchemeOperation && supported.SchemeOperation->OperationType == operationType) {
            return supported.Kind;
        }
    }
    return Nothing();
}

bool SupportsOperationIdempotency(NKikimrSchemeOp::EOperationType operationType) {
    return GetOperationUidKind(operationType).Defined();
}

bool SupportsSqlOperationIdempotency(TStringBuf writeMode) {
    for (const auto& supported : SupportedOperations) {
        if (supported.SchemeOperation && supported.SchemeOperation->SqlWriteMode == writeMode) {
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
        if (supported.SchemeOperation && supported.SchemeOperation->KqpOperationCase == operation.GetOperationCase()) {
            const auto& scheme = *supported.SchemeOperation;
            const auto& payload = (operation.*scheme.GetPayload)();
            return payload.GetOperationType() == scheme.OperationType ? &payload : nullptr;
        }
    }
    return nullptr;
}

} // namespace NKikimr::NSchemeShard
