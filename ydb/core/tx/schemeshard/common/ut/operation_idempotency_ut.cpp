#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSchemeShard {

Y_UNIT_TEST_SUITE(OperationUidSupport) {
    Y_UNIT_TEST(LegacyRpcUidSupportDoesNotEnableSql) {
        for (auto kind : {Ydb::TOperationId::EXPORT, Ydb::TOperationId::IMPORT,
                Ydb::TOperationId::BUILD_INDEX, Ydb::TOperationId::SET_NOT_NULL}) {
            UNIT_ASSERT(SupportsOperationUid(kind));
            UNIT_ASSERT(!SupportsSqlOperationIdempotency(kind));

            Ydb::Operations::OperationParams params;
            UNIT_ASSERT(GetUid(kind, params).empty());
            for (const auto& key : {TString(), TString("legacy UID / ключ"), TString("a\0b", 3)}) {
                (*params.mutable_labels())["uid"].assign(key.data(), key.size());
                UNIT_ASSERT_VALUES_EQUAL(GetUid(kind, params), key);
            }
        }
        for (TStringBuf mode : {"export", "import", "buildIndex", "setNotNull", ""}) {
            UNIT_ASSERT(!SupportsSqlOperationIdempotency(mode));
        }
        // Index construction suboperations do not accept the SQL UID envelope.
        UNIT_ASSERT(!SupportsOperationIdempotency(NKikimrSchemeOp::ESchemeOpCreateIndexBuild));
    }

    Y_UNIT_TEST(SchemeMappingsPreserveKindAndPayloadValidation) {
        using TKqpOperation = NKqpProto::TKqpSchemeOperation;
        struct TCase {
            Ydb::TOperationId::EKind Kind;
            NKikimrSchemeOp::EOperationType Type;
            TStringBuf SqlWriteMode;
            NKikimrSchemeOp::TModifyScheme* (TKqpOperation::*MutablePayload)();
        };
        const TCase cases[] = {
            {Ydb::TOperationId::FULL_BACKUP, NKikimrSchemeOp::ESchemeOpBackupBackupCollection,
                "backup", &TKqpOperation::MutableBackup},
            {Ydb::TOperationId::INCREMENTAL_BACKUP, NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection,
                "backupIncremental", &TKqpOperation::MutableBackupIncremental},
            {Ydb::TOperationId::RESTORE, NKikimrSchemeOp::ESchemeOpRestoreBackupCollection,
                "restore", &TKqpOperation::MutableRestore},
        };
        for (const auto& test : cases) {
            UNIT_ASSERT(SupportsOperationUid(test.Kind));
            UNIT_ASSERT(SupportsSqlOperationIdempotency(test.Kind));
            UNIT_ASSERT(SupportsOperationIdempotency(test.Type));
            UNIT_ASSERT(SupportsSqlOperationIdempotency(test.SqlWriteMode));

            TKqpOperation operation;
            auto* payload = (operation.*test.MutablePayload)();
            payload->SetOperationType(test.Type);
            UNIT_ASSERT(GetSchemeOperationForIdempotency(operation) == payload);

            payload->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
            UNIT_ASSERT(!GetSchemeOperationForIdempotency(operation));
            payload->SetOperationType(test.Type);
            operation.SetObjectType("TABLE");
            UNIT_ASSERT(!GetSchemeOperationForIdempotency(operation));
        }
    }

    Y_UNIT_TEST(UnlistedOperationsDoNotAcquireUidSupport) {
        for (auto kind : {Ydb::TOperationId::UNUSED, Ydb::TOperationId::OPERATION_DDL,
                Ydb::TOperationId::OPERATION_DML, Ydb::TOperationId::SCRIPT_EXECUTION,
                Ydb::TOperationId::SS_BG_TASKS, Ydb::TOperationId::COMPACTION,
                Ydb::TOperationId::ANALYZE, static_cast<Ydb::TOperationId::EKind>(1000)}) {
            UNIT_ASSERT(!SupportsOperationUid(kind));
            UNIT_ASSERT(!SupportsSqlOperationIdempotency(kind));
        }
        UNIT_ASSERT(!SupportsOperationIdempotency(NKikimrSchemeOp::ESchemeOpCreateTable));
        UNIT_ASSERT(!SupportsSqlOperationIdempotency("create"));
        UNIT_ASSERT(!GetSchemeOperationForIdempotency(NKqpProto::TKqpSchemeOperation{}));
    }
}

} // namespace NKikimr::NSchemeShard
