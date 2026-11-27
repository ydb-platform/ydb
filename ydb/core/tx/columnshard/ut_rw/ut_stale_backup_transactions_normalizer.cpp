#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/library/actors/struct_log/text_writer.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/stream/str.h>
#include <yql/essentials/ast/yql_ast_escaping.h>

#include <inttypes.h>

namespace NKikimr::NColumnShard {

using namespace Tests;
using namespace NTxUT;

namespace {

TString EscapeMiniKQLString(TStringBuf value) {
    TStringStream escaped;
    NYql::EscapeArbitraryAtom(value, '"', &escaped);
    return escaped.Str();
}

class TBackupNormalizerController: public NYDBTest::NColumnShard::TController {
public:
    NYDBTest::ILocalDBModifier::TPtr Modifier;

    NYDBTest::ILocalDBModifier::TPtr BuildLocalBaseModifier() const override {
        return Modifier;
    }
};

class TStaleBackupFixture {
private:
    const ui64 SchemeShardPathId;
    ui64 InternalPathId = 0;
    ui64 SchemaStep = 0;

public:
    NYDBTest::TControllers::TGuard<TBackupNormalizerController> Controller =
        NYDBTest::TControllers::RegisterCSControllerGuard<TBackupNormalizerController>();
    TTestBasicRuntime Runtime;
    TActorId Sender;

    explicit TStaleBackupFixture(ui64 schemeShardPathId = 1, bool generateInternalPathId = true, IOutputStream* logOutput = nullptr)
        : SchemeShardPathId(schemeShardPathId)
    {
        Controller->SetForcedGenerateInternalPathId(generateInternalPathId);
        if (logOutput) {
            Runtime.SetLogBackend(new TStreamLogBackend(logOutput));
        }
        TTester::Setup(Runtime);
        Runtime.GetAppData().ColumnShardConfig.SetGenerateInternalPathId(generateInternalPathId);
        const std::vector<NArrow::NTest::TTestColumn> schema = { NArrow::NTest::TTestColumn("key", TTypeInfo(NTypeIds::Uint64)) };
        SchemaStep = PrepareTablet(Runtime, SchemeShardPathId, schema).Val();
        Sender = Runtime.AllocateEdgeActor();
        InternalPathId = Controller->GetTheOnlyShard()
                             ->GetTablesManager()
                             .ResolveInternalPathIdVerified(TSchemeShardLocalPathId::FromRawValue(SchemeShardPathId), false)
                             .GetRawValue();

        // Simulate an upgrade from the previous sequential normalizer.
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "Value",
            Sprintf("'('('Id (Uint32 '%" PRIu32 ")))", static_cast<ui32>(Schema::EValueIds::LastNormalizerSequentialId)),
            Sprintf("'('('Digit (Uint64 '%" PRIu64 ")))", static_cast<ui64>(NOlap::ENormalizerSequentialId::CleanOrphanedOperations)));
    }

    TString AddTx(ui64 txId, NKikimrTxColumnShard::ETransactionKind kind = NKikimrTxColumnShard::TX_KIND_BACKUP, ui64 tableId = 0) {
        TString body;
        if (kind == NKikimrTxColumnShard::TX_KIND_BACKUP) {
            NKikimrTxColumnShard::TBackupTxBody backup;
            auto* task = backup.MutableBackupTask();
            task->SetTableId(tableId ? tableId : SchemeShardPathId);
            task->SetSnapshotStep(SchemaStep);
            task->SetSnapshotTxId(Max<ui64>());
            body = backup.SerializeAsString();
        } else if (kind == NKikimrTxColumnShard::TX_KIND_SCHEMA) {
            body = TTestSchema::DropTableTxBody(SchemeShardPathId, 2);
        } else {
            NKikimrTxColumnShard::TRestoreTxBody restore;
            restore.MutableRestoreTask()->SetTableId(SchemeShardPathId);
            body = restore.SerializeAsString();
        }
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TxInfo", Sprintf("'('('TxId (Uint64 '%" PRIu64 ")))", txId),
            Sprintf("'('('TxKind (Uint32 '%" PRIu32 ")) '('TxBody (String '%s)) '('MaxStep (Uint64 '%" PRIu64 ")))", static_cast<ui32>(kind),
                EscapeMiniKQLString(body).c_str(), Max<ui64>()));
        return body;
    }

    void SetCompletedTx(ui64 txId, bool success = true) {
        NKikimrTxColumnShard::TCompletedBackupTransaction completed;
        completed.SetTxId(txId);
        completed.MutableOpResult()->SetSuccess(success);
        if (!success) {
            completed.MutableOpResult()->SetExplain("Cancelled");
        }
        SetSerializedResult(completed.SerializeAsString());
    }

    void SetSerializedResult(const TString& result) {
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfoV1",
            Sprintf("'('('PathId (Uint64 '%" PRIu64 ")) '('SchemeShardLocalPathId (Uint64 '%" PRIu64 ")))", InternalPathId, SchemeShardPathId),
            Sprintf("'('('LastCompletedBackupTransaction (String '%s)))", EscapeMiniKQLString(result).c_str()));
    }

    void Reboot() {
        RebootTablet(Runtime, TTestTxConfig::TxTablet0, Sender);
    }

    void KeepOnlyLegacyTable(bool withSchemeShardPathId) {
        EraseLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfoV1",
            Sprintf("'('('PathId (Uint64 '%" PRIu64 ")) '('SchemeShardLocalPathId (Uint64 '%" PRIu64 ")))", InternalPathId, SchemeShardPathId));
        const TString key = Sprintf("'('('PathId (Uint64 '%" PRIu64 ")))", InternalPathId);
        EraseLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfo", key);
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfo", key,
            withSchemeShardPathId ? Sprintf("'('('SchemeShardLocalPathId (Uint64 '%" PRIu64 ")))", SchemeShardPathId)
                                  : TString("'('('TieringUsage (String '\"\")))"));
    }

    void KeepOnlyReadOnlyTable() {
        UpdateLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfoV1",
            Sprintf("'('('PathId (Uint64 '%" PRIu64 ")) '('SchemeShardLocalPathId (Uint64 '%" PRIu64 ")))", InternalPathId, SchemeShardPathId),
            "'('('IsReadOnly (Bool 'true)))");
        EraseLocalDbTableRow(Runtime, TTestTxConfig::TxTablet0, "TableInfo", Sprintf("'('('PathId (Uint64 '%" PRIu64 ")))", InternalPathId));
    }

    void AssertTxCount(ui64 count) {
        UNIT_ASSERT_VALUES_EQUAL(CountTxInfoRows(Runtime), count);
        UNIT_ASSERT_VALUES_EQUAL(Controller->GetTheOnlyShard()->GetProgressTxController().GetTxs().size(), count);
    }

    void AssertCompletion(ui64 txId, bool success) {
        ForwardToTablet(Runtime, TTestTxConfig::TxTablet0, Sender, new TEvColumnShard::TEvNotifyTxCompletion(txId));
        auto response = Runtime.GrabEdgeEvent<TEvColumnShard::TEvNotifyTxCompletionResult>(Sender);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT(record.HasOpResult());
        UNIT_ASSERT_VALUES_EQUAL(record.GetOpResult().GetSuccess(), success);
        UNIT_ASSERT_VALUES_EQUAL(record.GetOpResult().GetExplain(), success ? "" : "Cancelled");
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(StaleBackupTransactionsNormalizer) {
    Y_UNIT_TEST_TWIN(LogsRecoverableTxInfo, withMetadata) {
        const TActorId source(42, 7, Max<ui64>() - 1, 123);
        const TString seqNo("\0\xff\"\\\n", 5);

        class TMetadataModifier: public NYDBTest::ILocalDBModifier {
        public:
            const TActorId Source;
            const TString SeqNo;

            TMetadataModifier(const TActorId& source, const TString& seqNo)
                : Source(source)
                , SeqNo(seqNo)
            {
            }

            void Apply(NTabletFlatExecutor::TTransactionContext& txc) const override {
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::TxInfo>().Key(111).Update(NIceDb::TUpdate<Schema::TxInfo::PlanStep>(Max<ui64>() - 2),
                    NIceDb::TUpdate<Schema::TxInfo::Source>(Source), NIceDb::TUpdate<Schema::TxInfo::Cookie>(Max<ui64>()),
                    NIceDb::TUpdate<Schema::TxInfo::SeqNo>(SeqNo));
            }
        };

        TStringStream logs;
        TStaleBackupFixture shard(1, true, &logs);
        shard.SetCompletedTx(111);
        const TString body = shard.AddTx(111);
        if (withMetadata) {
            shard.Controller->Modifier = std::make_shared<TMetadataModifier>(source, seqNo);
        }
        shard.Reboot();
        shard.AssertTxCount(0);

        const TString marker = "txInfoJson=";
        const size_t markerPos = logs.Str().find(marker);
        UNIT_ASSERT_C(markerPos != TString::npos, "Missing TxInfo recovery record");
        const size_t valuePos = markerPos + marker.size();
        const TString field = logs.Str().substr(valuePos, logs.Str().find('\n', valuePos) - valuePos);
        const auto json = NActors::NStructuredLog::TTextWriter::UnescapeFieldValue(field);
        UNIT_ASSERT(json);
        NJson::TJsonValue snapshot;
        UNIT_ASSERT(NJson::ReadJsonTree(*json, &snapshot));
        UNIT_ASSERT_VALUES_EQUAL(snapshot["Version"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(snapshot["TabletId"].GetUInteger(), TTestTxConfig::TxTablet0);
        const auto& row = snapshot["TxInfo"];
        UNIT_ASSERT_VALUES_EQUAL(row.GetMapSafe().size(), 8);
        UNIT_ASSERT_VALUES_EQUAL(row["TxId"].GetUInteger(), 111);
        UNIT_ASSERT_VALUES_EQUAL(row["TxKind"].GetUInteger(), static_cast<ui64>(NKikimrTxColumnShard::TX_KIND_BACKUP));
        UNIT_ASSERT_VALUES_EQUAL(Base64Decode(row["TxBodyBase64"].GetString()), body);
        UNIT_ASSERT_VALUES_EQUAL(row["MaxStep"].GetUInteger(), Max<ui64>());
        if (withMetadata) {
            UNIT_ASSERT_VALUES_EQUAL(row["PlanStep"].GetUInteger(), Max<ui64>() - 2);
            UNIT_ASSERT_VALUES_EQUAL(row["Cookie"].GetUInteger(), Max<ui64>());
            UNIT_ASSERT_VALUES_EQUAL(Base64Decode(row["SeqNoBase64"].GetString()), seqNo);
            const TActorId restoredSource(row["Source"]["RawX1"].GetUInteger(), row["Source"]["RawX2"].GetUInteger());
            UNIT_ASSERT_VALUES_EQUAL(restoredSource, source);
        } else {
            UNIT_ASSERT(row["PlanStep"].IsNull());
            UNIT_ASSERT(row["Source"].IsNull());
            UNIT_ASSERT(row["Cookie"].IsNull());
            UNIT_ASSERT(row["SeqNoBase64"].IsNull());
        }
    }

    Y_UNIT_TEST_TWIN(CompletedBackupCleanedAndResultPreserved, success) {
        TStaleBackupFixture shard;
        shard.SetCompletedTx(111, success);
        shard.AddTx(111);
        UNIT_ASSERT_VALUES_EQUAL(CountTxInfoRows(shard.Runtime), 1);

        shard.Reboot();

        shard.AssertTxCount(0);
        shard.AssertCompletion(111, success);
    }

    Y_UNIT_TEST(RunsOnlyOnce) {
        TStaleBackupFixture shard;
        shard.SetCompletedTx(111);
        shard.AddTx(111);
        shard.Reboot();
        shard.AssertTxCount(0);
        shard.AssertCompletion(111, true);

        shard.SetCompletedTx(112);
        shard.AddTx(112);
        shard.Reboot();
        shard.AssertTxCount(1);
        shard.Reboot();
        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST(UnfinishedBackupWithoutSessionPreserved) {
        TStaleBackupFixture shard;
        shard.SetCompletedTx(111);
        shard.AddTx(111);
        shard.AddTx(112);

        shard.Reboot();

        shard.AssertTxCount(1);
        UNIT_ASSERT(shard.Controller->GetTheOnlyShard()->GetProgressTxController().GetTxs().contains(112));
        shard.AssertCompletion(111, true);
    }

    Y_UNIT_TEST(OtherTransactionKindsPreserved) {
        TStaleBackupFixture shard;
        shard.SetCompletedTx(111);
        shard.AddTx(111, NKikimrTxColumnShard::TX_KIND_RESTORE);

        shard.Reboot();

        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST(MissingCompletionResultPreserved) {
        TStaleBackupFixture shard;
        shard.AddTx(111);

        shard.Reboot();

        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST(BackupsOfRemovedTablesCleanedAndSchemaPreserved) {
        TStaleBackupFixture shard(9317);
        const std::vector<std::pair<ui64, ui64>> backups = {
            { 283111830367628, 11437 },
            { 283111830369014, 12129 },
            { 564564958407245, 10053 },
            { 564705533976899, 12838 },
            { 564705533977594, 13185 },
        };
        for (const auto& [txId, tableId] : backups) {
            shard.AddTx(txId, NKikimrTxColumnShard::TX_KIND_BACKUP, tableId);
        }
        const ui64 schemaTxId = 846530786266161;
        shard.AddTx(schemaTxId, NKikimrTxColumnShard::TX_KIND_SCHEMA);
        UNIT_ASSERT_VALUES_EQUAL(CountTxInfoRows(shard.Runtime), 6);

        shard.Reboot();

        shard.AssertTxCount(1);
        UNIT_ASSERT(shard.Controller->GetTheOnlyShard()->GetProgressTxController().GetTxs().contains(schemaTxId));
        shard.Reboot();
        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST_TWIN(LegacyTablePreserved, separatePathIds) {
        TStaleBackupFixture shard(9317, separatePathIds);
        shard.KeepOnlyLegacyTable(separatePathIds);
        shard.AddTx(111);

        shard.Reboot();

        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST(ReadOnlyTablePreserved) {
        TStaleBackupFixture shard;
        shard.KeepOnlyReadOnlyTable();
        shard.AddTx(111);

        shard.Reboot();

        shard.AssertTxCount(1);
    }

    Y_UNIT_TEST(InvalidCompletionResultsPreserved) {
        NKikimrTxColumnShard::TCompletedBackupTransaction noResult;
        noResult.SetTxId(111);
        NKikimrTxColumnShard::TCompletedBackupTransaction noTxId;
        noTxId.MutableOpResult()->SetSuccess(true);
        for (const TString& result : { TString("invalid protobuf"), TString(), noResult.SerializeAsString(), noTxId.SerializeAsString() }) {
            TStaleBackupFixture shard;
            shard.AddTx(111);
            shard.SetSerializedResult(result);
            shard.Reboot();
            shard.AssertTxCount(1);
        }
    }
}

}   // namespace NKikimr::NColumnShard
