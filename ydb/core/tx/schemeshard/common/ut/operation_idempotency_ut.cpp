#include <ydb/core/tx/schemeshard/common/operation_idempotency.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>

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
            UNIT_ASSERT(*GetOperationUidKind(test.Type) == test.Kind);
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

Y_UNIT_TEST_SUITE(OperationUidAdmission) {
    using TAdmission = TOperationUidAdmission;
    using EDecision = TAdmission::EDecision;
    using EPolicy = TAdmission::EDuplicatePolicy;

    Y_UNIT_TEST(MissingUidDoesNotLookupButStillPersistsOperation) {
        bool persisted = false;
        auto admission = TAdmission::Prepare({Ydb::TOperationId::EXPORT, {}}, EPolicy::Replay,
            [](const auto&) -> TMaybe<TOperationUidRecord> {
                UNIT_FAIL("An empty legacy UID must not be looked up");
                return Nothing();
            });
        UNIT_ASSERT(admission.Commit(true, [&] { persisted = true; }));
        UNIT_ASSERT(persisted);
    }

    Y_UNIT_TEST(FailedAdmissionDoesNotBindUid) {
        auto admission = TAdmission::Prepare({Ydb::TOperationId::FULL_BACKUP, "uid"}, EPolicy::Replay,
            [](const auto&) -> TMaybe<TOperationUidRecord> { return Nothing(); });
        UNIT_ASSERT(admission.GetDecision() == EDecision::Proceed);
        UNIT_ASSERT(!admission.Commit(false, [] { UNIT_FAIL("Failed work must not persist a UID"); }));
    }

    Y_UNIT_TEST(LegacyRecordsKeepTheirDuplicatePolicy) {
        for (auto kind : {Ydb::TOperationId::EXPORT, Ydb::TOperationId::IMPORT,
                Ydb::TOperationId::BUILD_INDEX, Ydb::TOperationId::SET_NOT_NULL}) {
            const bool replay = kind == Ydb::TOperationId::EXPORT || kind == Ydb::TOperationId::IMPORT;
            // Existing records have no original request identity. New code must
            // neither require one nor attach the retry's body to that record.
            const TOperationUidRecord legacy{42, TPathId(1, 2), {}, {}};
            auto admission = TAdmission::Prepare({kind, "legacy UID / ключ"}, replay ? EPolicy::Replay : EPolicy::Reject,
                [&](const auto&) -> TMaybe<TOperationUidRecord> { return legacy; },
                [&](const auto& stored) {
                    UNIT_ASSERT(replay); // Reject policy must not invoke identity checks.
                    return CompareOperationUid({stored.DomainPathId, {}, {}}, {TPathId(1, 2), {}, {}});
                });
            UNIT_ASSERT(admission.GetDecision() == (replay ? EDecision::Replay : EDecision::AlreadyExists));
            UNIT_ASSERT_VALUES_EQUAL(admission.GetOperationId(), 42);
            UNIT_ASSERT(!admission.Commit(true, [] { UNIT_FAIL("Duplicates must not create new records"); }));
        }
    }

    Y_UNIT_TEST(IdentityChecksKeepOwnerBeforeBodyAndPreserveDomainConflicts) {
        const TOperationUidRecord stored{42, TPathId(1, 2), "owner", "original"};
        const auto check = [&](const TOperationUidIdentity& requested, EDecision expected) {
            auto admission = TAdmission::Prepare({Ydb::TOperationId::RESTORE, "uid"}, EPolicy::Replay,
                [&](const auto&) -> TMaybe<TOperationUidRecord> { return stored; },
                [&](const auto& receipt) {
                    return CompareOperationUid(
                        {receipt.DomainPathId, TStringBuf(receipt.UserSID), TStringBuf(receipt.RequestBody)}, requested);
                });
            UNIT_ASSERT(admission.GetDecision() == expected);
            UNIT_ASSERT(!admission.Commit(true, [] { UNIT_FAIL("Existing UID must not be rebound"); }));
        };
        check({{}, "other", "different"}, EDecision::OwnerMismatch);
        check({{}, "owner", "different"}, EDecision::RequestMismatch);
        check({{}, "owner", "original"}, EDecision::Replay);
        check({TPathId(1, 3), {}, {}}, EDecision::DomainMismatch);
    }

    Y_UNIT_TEST(OperationKindsKeepIndependentNamespaces) {
        TMap<TOperationUidKey, TOperationUidRecord> records;
        for (auto kind : {Ydb::TOperationId::EXPORT, Ydb::TOperationId::IMPORT,
                Ydb::TOperationId::BUILD_INDEX, Ydb::TOperationId::SET_NOT_NULL,
                Ydb::TOperationId::FULL_BACKUP, Ydb::TOperationId::INCREMENTAL_BACKUP, Ydb::TOperationId::RESTORE}) {
            const TOperationUidKey key{kind, "same uid"};
            auto admission = TAdmission::Prepare(key, EPolicy::Reject,
                [&](const auto& uid) -> TMaybe<TOperationUidRecord> {
                    if (const auto* record = FindOperationByUid(records, uid)) {
                        return *record;
                    }
                    return Nothing();
                });
            UNIT_ASSERT(admission.Commit(true, [&] {
                UNIT_ASSERT(records.emplace(key, TOperationUidRecord{42, {}, {}, {}}).second);
            }));
        }
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 7);
    }
}

} // namespace NKikimr::NSchemeShard
