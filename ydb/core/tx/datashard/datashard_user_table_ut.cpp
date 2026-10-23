#include "datashard_user_table.h"
#include "datashard_impl.h"
#include "key_validator.h"
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDataShard {
namespace {

NKikimrSchemeOp::TTableDescription GeneratedTableDescription() {
    NKikimrSchemeOp::TTableDescription description;
    description.SetName("Table");
    description.SetPath("/Root/Table");
    description.SetTableSchemaVersion(10);
    for (const auto& [id, name] : {std::pair{17u, "key"}, {23u, "a"}, {41u, "g"}}) {
        auto* column = description.AddColumns();
        column->SetId(id);
        column->SetName(name);
        column->SetTypeId(NScheme::NTypeIds::Int64);
    }
    description.AddKeyColumnIds(17);
    auto* generated = description.MutableColumns(2);
    generated->SetIsBuildInProgress(true);
    auto* expression = generated->MutableDefaultFromExpression();
    expression->SetExprText("a + key");
    expression->AddDependencyColumnNames("a");
    expression->AddDependencyColumnNames("key");
    expression->SetStored(true);
    return description;
}

const NKikimrSchemeOp::TColumnDescription& FindColumn(
        const NKikimrSchemeOp::TTableDescription& description, ui32 id) {
    for (const auto& column : description.GetColumns()) {
        if (column.GetId() == id) {
            return column;
        }
    }
    ythrow yexception() << "Column " << id << " is missing";
}

Tests::TServer::TPtr CreateGeneratedAlterServer(TPortManager& ports) {
    Tests::TServerSettings settings(ports.GetPort(2134));
    settings.SetDomainName("Root").SetUseRealThreads(false);
    settings.FeatureFlags.SetEnableGeneratedStored(true);
    settings.FeatureFlags.SetEnableAddColumnGeneratedStored(true);
    settings.AppConfig->MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return new Tests::TServer(settings);
}

void SendGeneratedAlter(Tests::TServer::TPtr server, TActorId sender, const TString& sql) {
    auto& runtime = *server->GetRuntime();
    auto request = MakeSQLRequest(sql, false);
    request->Record.MutableRequest()->SetDatabase("/Root");
    runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
}

} // namespace

Y_UNIT_TEST_SUITE(TDataShardGeneratedSchema) {
    Y_UNIT_TEST(GeneratedHiddenSchemaRoundTrip) {
        TUserTable table(100, GeneratedTableDescription(), 0);
        // Path changes rebuild the serialized schema from the parsed columns.
        table.SetPath("/Root/RenamedTable");
        NKikimrSchemeOp::TTableDescription serialized;
        table.GetSchema(serialized);
        const auto& hidden = FindColumn(serialized, 41);
        UNIT_ASSERT(hidden.HasDefaultFromExpression());
        UNIT_ASSERT(hidden.GetDefaultFromExpression().GetStored());
        UNIT_ASSERT_VALUES_EQUAL(hidden.GetDefaultFromExpression().GetExprText(), "a + key");
        UNIT_ASSERT_VALUES_EQUAL(hidden.GetDefaultFromExpression().DependencyColumnNamesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(hidden.GetDefaultFromExpression().GetDependencyColumnNames(0), "a");
        UNIT_ASSERT_VALUES_EQUAL(hidden.GetDefaultFromExpression().GetDependencyColumnNames(1), "key");
        UNIT_ASSERT(hidden.GetIsBuildInProgress());

        TUserTable rebooted(100, serialized, 0);
        NKikimrSchemeOp::TTableDescription finalize;
        finalize.SetName("RenamedTable");
        finalize.SetTableSchemaVersion(11);
        auto* published = finalize.AddColumns();
        published->CopyFrom(hidden);
        published->SetIsBuildInProgress(false);
        TUserTable completed(rebooted, finalize);
        completed.GetSchema(serialized);
        const auto& visible = FindColumn(serialized, 41);
        UNIT_ASSERT(visible.GetDefaultFromExpression().GetStored());
        UNIT_ASSERT(!visible.GetIsBuildInProgress());
        UNIT_ASSERT_VALUES_EQUAL(visible.GetDefaultFromExpression().GetExprText(), "a + key");
        UNIT_ASSERT(completed.HasStoredGeneratedColumns());
        UNIT_ASSERT(completed.HasStoredGeneratedColumnDependency(17));
        UNIT_ASSERT(completed.HasStoredGeneratedColumnDependency(23));
        UNIT_ASSERT(!completed.HasStoredGeneratedColumnDependency(41));
        UNIT_ASSERT(!completed.HasStoredGeneratedColumnDependency(999));
    }

    Y_UNIT_TEST(GeneratedMetadataSurvivesIndependentAlter) {
        TUserTable table(100, GeneratedTableDescription(), 0);
        NKikimrSchemeOp::TTableDescription delta;
        delta.SetName("Table");
        delta.SetTableSchemaVersion(11);
        auto* ordinary = delta.AddColumns();
        ordinary->SetId(57);
        ordinary->SetName("ordinary");
        ordinary->SetTypeId(NScheme::NTypeIds::Int64);
        ordinary->MutableDefaultFromLiteral()->mutable_type()->set_type_id(Ydb::Type::INT64);
        ordinary->MutableDefaultFromLiteral()->mutable_value()->set_int64_value(123);
        TUserTable altered(table, delta);
        NKikimrSchemeOp::TTableDescription serialized;
        altered.GetSchema(serialized);
        UNIT_ASSERT(FindColumn(serialized, 41).GetDefaultFromExpression().GetStored());
        UNIT_ASSERT(FindColumn(serialized, 41).GetIsBuildInProgress());
        UNIT_ASSERT(!FindColumn(serialized, 57).HasDefaultFromExpression());
    }

    Y_UNIT_TEST_TWIN(GeneratedOldRequestDuringBuild, Prepared) {
        TPortManager ports;
        auto server = CreateGeneratedAlterServer(ports);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        const auto writeSender = runtime.AllocateEdgeActor();
        const auto ddlSender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "Table");
        const auto columns = TShardedTableOptions::DefaultColumns();
        UpsertOneKeyValue(runtime, sender, shards.front(), tableId, columns, 1, 3, {},
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);

        const ui64 txId = 10000;
        NActors::TBlockEvents<NEvents::TDataEvents::TEvWrite> oldWrites(runtime, [&](const auto& ev) {
            return ev->Sender == writeSender && ev->Get()->Record.GetTxId() == txId;
        });
        auto request = MakeWriteRequestOneKeyValue(txId,
            Prepared ? NKikimrDataEvents::TEvWrite::MODE_PREPARE : NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWrite::TOperation::OPERATION_UPSERT, tableId, columns, 1, 9);
        runtime.SendToPipe(shards.front(), writeSender, request.release());
        runtime.WaitFor("old-schema write to reach the shard", [&] { return !oldWrites.empty(); });

        bool ddlFinished = false;
        TString ddlResponse;
        auto responseObserver = runtime.AddObserver<NKqp::TEvKqp::TEvQueryResponse>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == ddlSender) {
                ddlFinished = true;
                ddlResponse = ev->Get()->Record.DebugString();
            }
        });
        NActors::TBlockEvents<TEvDataShard::TEvUploadRowsRequest> fills(runtime, [](const auto& ev) {
            return ev->Get()->Record.HasGeneratedColumnFillContext();
        });
        SendGeneratedAlter(server, ddlSender,
            "ALTER TABLE `/Root/Table` ADD COLUMN g Uint32 GENERATED ALWAYS AS (value + 1u) STORED;");
        runtime.WaitFor("hidden generated column fill or ALTER failure", [&] {
            return !fills.empty() || ddlFinished;
        });
        UNIT_ASSERT_C(!fills.empty(), "ALTER finished before generated fill: " << ddlResponse);
        UNIT_ASSERT_GT(fills.front()->Get()->Record.GetSchemaVersion(), tableId.SchemaVersion);

        // This request was created before ALTER, but is first admitted after the
        // hidden schema and the fill snapshot have been installed.
        oldWrites.Stop().Unblock();
        WaitForWriteCompleted(runtime, writeSender, NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED);
        fills.Stop().Unblock();
        auto done = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(ddlSender);
        UNIT_ASSERT_VALUES_EQUAL_C(done->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
            done->Get()->Record.DebugString());
        const auto publishedTableId = ResolveTableId(server, sender, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(server, shards, publishedTableId), "key = 1, value = 3, g = 4\n");
    }

    Y_UNIT_TEST(GeneratedPreparedWriteOrdersBeforeAlter) {
        TPortManager ports;
        auto server = CreateGeneratedAlterServer(ports);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        const auto writeSender = runtime.AllocateEdgeActor();
        const auto ddlSender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "Table");
        const auto columns = TShardedTableOptions::DefaultColumns();
        UpsertOneKeyValue(runtime, sender, shards.front(), tableId, columns, 1, 3, {},
            NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        const auto shardActor = ResolveTablet(runtime, shards.front());

        const ui64 txId = 10000;
        auto prepared = UpsertOneKeyValue(runtime, writeSender, shards.front(), tableId, columns, 1, 9, txId,
            NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        UNIT_ASSERT_VALUES_EQUAL(prepared.GetDomainCoordinators().size(), 1);
        NActors::TBlockEvents<TEvTxProcessing::TEvPlanStep> plans(runtime, [&](const auto& ev) {
            if (ev->GetRecipientRewrite() == shardActor) {
                for (const auto& transaction : ev->Get()->Record.GetTransactions()) {
                    if (transaction.GetTxId() == txId) {
                        return true;
                    }
                }
            }
            return false;
        });
        SendProposeToCoordinator(runtime, writeSender, shards, {
            .TxId = txId,
            .Coordinator = prepared.GetDomainCoordinators(0),
            .MinStep = prepared.GetMinStep(),
            .MaxStep = prepared.GetMaxStep(),
        });
        runtime.WaitFor("prepared write plan to reach the shard", [&] { return !plans.empty(); });

        bool hiddenAlterProposed = false;
        auto schemaObserver = runtime.AddObserver<TEvDataShard::TEvProposeTransaction>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (ev->GetRecipientRewrite() != shardActor || record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) {
                return;
            }
            NKikimrTxDataShard::TFlatSchemeTransaction transaction;
            UNIT_ASSERT(transaction.ParseFromString(record.GetTxBody()));
            if (transaction.HasAlterTable()) {
                for (const auto& column : transaction.GetAlterTable().GetColumns()) {
                    if (column.GetName() == "g" && column.GetIsBuildInProgress()
                        && column.GetDefaultFromExpression().GetStored()) {
                        hiddenAlterProposed = true;
                    }
                }
            }
        });
        bool ddlFinished = false;
        TString ddlResponse;
        auto responseObserver = runtime.AddObserver<NKqp::TEvKqp::TEvQueryResponse>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == ddlSender) {
                ddlFinished = true;
                ddlResponse = ev->Get()->Record.DebugString();
            }
        });
        SendGeneratedAlter(server, ddlSender,
            "ALTER TABLE `/Root/Table` ADD COLUMN g Uint32 GENERATED ALWAYS AS (value + 1u) STORED;");
        runtime.WaitFor("hidden-column ALTER proposal or failure", [&] { return hiddenAlterProposed || ddlFinished; });
        UNIT_ASSERT_C(hiddenAlterProposed, "ALTER finished before hidden schema proposal: " << ddlResponse);

        // A prepared nonvolatile write must be ordered before the schema change;
        // ALTER cannot install its hidden column while this data plan is held.
        const auto [tables, ownerId] = GetTablesByPathId(server, shards.front());
        UNIT_ASSERT_VALUES_EQUAL(tables.at(tableId.PathId).GetDescription().GetTableSchemaVersion(), tableId.SchemaVersion);
        plans.Stop().Unblock();
        auto completed = WaitForWriteCompleted(runtime, writeSender);
        UNIT_ASSERT_VALUES_EQUAL(completed.GetTxId(), txId);
        auto done = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(ddlSender);
        UNIT_ASSERT_VALUES_EQUAL_C(done->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
            done->Get()->Record.DebugString());
        const auto publishedTableId = ResolveTableId(server, sender, "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(ReadTable(server, shards, publishedTableId), "key = 1, value = 9, g = 10\n");
    }

    Y_UNIT_TEST(GeneratedRejectsStaleAndZeroVersionWrites) {
        TPortManager ports;
        Tests::TServer::TPtr server = new Tests::TServer(Tests::TServerSettings(ports.GetPort(2134))
            .SetDomainName("Root").SetUseRealThreads(false));
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        const auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "Table");
        Upsert(runtime, sender, shards.front(), tableId, TShardedTableOptions::DefaultColumns(),
            1, {}, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
            NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        const auto before = ReadTable(server, shards, tableId);

        TDataShard* shard = nullptr;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvGetInfoRequest::EventType) {
                shard = dynamic_cast<TDataShard*>(runtime.FindActor(ev->GetRecipientRewrite()));
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        runtime.SendToPipe(shards.front(), sender, new TEvDataShard::TEvGetInfoRequest());
        runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(sender);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        UNIT_ASSERT(shard);

        TUserTable::TPtr original = new TUserTable(*shard->GetUserTables().at(tableId.PathId.LocalPathId));
        NKikimrSchemeOp::TTableDescription description;
        original->GetSchema(description);
        for (auto& column : *description.MutableColumns()) {
            if (column.GetName() == "value") {
                auto* expression = column.MutableDefaultFromExpression();
                expression->SetStored(true);
                expression->SetExprText("key + 1");
                expression->AddDependencyColumnNames("key");
                column.SetIsBuildInProgress(true);
            }
        }
        TUserTable::TPtr stored = new TUserTable(original->LocalTid, description, original->ShadowTid);
        shard->SetUserTable(tableId.PathId, stored);

        NTable::TScheme scheme;
        auto& physical = scheme.Tables.emplace(stored->LocalTid,
            NTable::TScheme::TTableInfo("Table", stored->LocalTid)).first->second;
        physical.KeyColumns = stored->KeyColumnIds;
        for (const auto& [id, column] : stored->Columns) {
            physical.Columns.emplace(id, NTable::TColumn(column.Name, id, column.Type, column.TypeMod));
        }
        const auto current = stored->GetTableSchemaVersion();
        UNIT_ASSERT(current > 0);
        const ui32 key = 1;
        const TVector<TCell> cells{TCell::Make(key)};
        const TTableRange range(cells);
        auto validate = [&](ui64 version, TKeyDesc::ERowOperation operation) {
            TKeyValidator validator(*shard);
            auto keyDescription = MakeHolder<TKeyDesc>(
                TTableId(tableId.PathId.OwnerId, tableId.PathId.LocalPathId, version),
                range, operation, stored->KeyColumnTypes, TVector<TKeyDesc::TColumnOp>{});
            validator.GetInfo().Keys.emplace_back(std::move(keyDescription), operation != TKeyDesc::ERowOperation::Read);
            const TKeyValidator::TValidateOptions options(0, 0, false, true,
                operation != TKeyDesc::ERowOperation::Read, scheme);
            return std::get<0>(validator.ValidateKeys(options));
        };
        using EResult = NMiniKQL::IEngineFlat::EResult;
        ui64 transactionId = 10000;
        for (bool hidden : {true, false}) {
            if (!hidden) {
                for (const auto& [id, column] : stored->Columns) {
                    if (column.IsStoredGenerated()) {
                        stored->FinishColumnBuild(id);
                    }
                }
            }
            for (const auto operation : {TKeyDesc::ERowOperation::Update, TKeyDesc::ERowOperation::Erase}) {
                UNIT_ASSERT(validate(0, operation) != EResult::Ok);
                UNIT_ASSERT(validate(current + 1, operation) == EResult::SchemeChanged);
                UNIT_ASSERT(validate(current, operation) == EResult::Ok);
            }
            UNIT_ASSERT(validate(0, TKeyDesc::ERowOperation::Read) == EResult::Ok);
            for (const ui64 version : {ui64(0), current + 1}) {
                const TTableId requested(tableId.PathId.OwnerId, tableId.PathId.LocalPathId, version);
                // DataEvents already fences versions. Keep this regression alongside
                // the generic and CommitWrites paths used by older/internal callers.
                Upsert(runtime, sender, shards.front(), requested, TShardedTableOptions::DefaultColumns(),
                    3, {}, NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE,
                    NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED);

                NKikimrTxDataShard::TCommitWritesTransaction commit;
                commit.MutableTableId()->SetOwnerId(requested.PathId.OwnerId);
                commit.MutableTableId()->SetTableId(requested.PathId.LocalPathId);
                commit.MutableTableId()->SetSchemaVersion(version);
                commit.SetWriteTxId(123);
                auto request = MakeHolder<TEvDataShard::TEvProposeTransaction>(
                    NKikimrTxDataShard::TX_KIND_COMMIT_WRITES, sender, ++transactionId, commit.SerializeAsString());
                runtime.SendToPipe(shards.front(), sender, request.Release());
                auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.ErrorSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetError(0).GetKind(), NKikimrTxDataShard::TError::SCHEME_CHANGED);
                UNIT_ASSERT_VALUES_EQUAL(ReadTable(server, shards, tableId), before);
            }
        }
        shard->SetUserTable(tableId.PathId, original);
        UNIT_ASSERT(validate(0, TKeyDesc::ERowOperation::Update) == EResult::Ok);
    }
}

} // namespace NKikimr::NDataShard
