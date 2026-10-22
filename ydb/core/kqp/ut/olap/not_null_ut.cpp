#include "not_null_test_enums.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/record_batch.h>

namespace NKikimr::NKqp {
using namespace NYdb;
using namespace NSetNotNull;

namespace {

enum class EWriteValue {
    Filled,
    Null,
    Missing,
};

TKikimrSettings MakeSettings(bool enabled = true) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    settings.AppConfig.MutableFeatureFlags()->SetEnableSetColumnConstraint(true);
    settings.AppConfig.MutableFeatureFlags()->SetEnableColumnShardSetNotNull(enabled);
    settings.AppConfig.MutableColumnShardConfig()->SetBulkUpsertRequireAllColumns(false);
    return settings;
}

struct TFixture {
    TKikimrRunner Kikimr;
    NQuery::TQueryClient Client;

    explicit TFixture(ETableKind tableKind = ETableKind::Column, bool enabled = true, ui32 shardCount = 4)
        : Kikimr(MakeSettings(enabled))
        , Client(Kikimr.RunCall([&] { return Kikimr.GetQueryClient(); }))
    {
        TStringBuilder create;
        create << R"(
            CREATE TABLE `/Root/t` (
                k Uint64 NOT NULL, v Utf8, w Uint64, PRIMARY KEY(k)
            )
        )";
        if (tableKind == ETableKind::Column) {
            create << "PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = " << shardCount << ");";
        } else {
            create << "WITH (STORE = ROW, UNIFORM_PARTITIONS = " << shardCount << ");";
        }
        Success(create, true);
    }

    auto Execute(const TString& sql, bool ddl = false, const NQuery::TExecuteQuerySettings& settings = {}) {
        return Kikimr.RunCall([&] {
            return Client.ExecuteQuery(sql,
                ddl ? NQuery::TTxControl::NoTx() : NQuery::TTxControl::BeginTx().CommitTx(), settings).GetValueSync();
        });
    }

    void Success(const TString& sql, bool ddl = false) {
        const auto result = Execute(sql, ddl);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto Failure(const TString& sql, bool ddl = false) {
        const auto result = Execute(sql, ddl,
            NQuery::TExecuteQuerySettings().RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0)));
        UNIT_ASSERT_C(!result.IsSuccess(), sql);
        return result;
    }

    TStatus Write(EWriteMode mode, ui64 key, EWriteValue value) {
        if (mode == EWriteMode::BulkUpsert) {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct()
                .AddMember("k").Uint64(key)
                .AddMember("w").Uint64(7);
            if (value == EWriteValue::Filled) {
                rows.AddMember("v").Utf8("filled");
            } else if (value == EWriteValue::Null) {
                rows.AddMember("v").EmptyOptional(EPrimitiveType::Utf8);
            }
            rows.EndStruct().EndList();
            return Kikimr.RunCall([&] {
                return Kikimr.GetTableClient().BulkUpsert("/Root/t", rows.Build()).GetValueSync();
            });
        }
        TStringBuilder query;
        query << ToString(mode) << " INTO `/Root/t` (k, w";
        if (value != EWriteValue::Missing) {
            query << ", v";
        }
        query << ") VALUES (" << key << "u, 7u";
        if (value != EWriteValue::Missing) {
            query << (value == EWriteValue::Filled ? ", 'filled'u" : ", NULL");
        }
        query << ");";
        return Execute(query);
    }

    void CheckRows(const TString& expected) {
        auto result = Execute("SELECT k, v FROM `/Root/t` ORDER BY k;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
    }
};

const TString SetNotNull = "ALTER TABLE `/Root/t` ALTER COLUMN v SET NOT NULL;";

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapSetNotNull) {
    Y_UNIT_TEST(TallTable, ENullPosition) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TFixture f(ETableKind::Column, true, 1);
        constexpr ui64 rowCount = 100000;
        constexpr ui64 rowsPerWrite = 10000;
        const auto position = Arg<0>();
        const ui64 nullKey = position == ENullPosition::None ? rowCount : position == ENullPosition::First ? 0 : rowCount - 1;
        for (ui64 begin = 0; begin < rowCount; begin += rowsPerWrite) {
            f.Success(TStringBuilder() << R"(
                $rows = ListMap(ListFromRange()" << begin << "ul, " << begin + rowsPerWrite << R"(ul),
                    ($x) -> { RETURN AsStruct($x AS item); });
                UPSERT INTO `/Root/t` (k, v)
                SELECT item, IF(item = )" << nullKey << R"(ul, CAST(NULL AS Utf8),
                    CAST(')" << TString(128, 'x') << R"(' AS Utf8)) FROM AS_TABLE($rows);
            )");
        }

        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TActorId validator;
        ui64 scannedRows = 0;
        ui32 batches = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType) {
                validator = ev->Sender;
                // Make the boundary keys deterministic despite asynchronous portion reads.
                ev->Get<TEvDataShard::TEvKqpScan>()->Record.SetReverse(false);
            } else if (ev->GetTypeRewrite() == TEvKqpCompute::TEvScanData::EventType && ev->Recipient == validator) {
                if (const auto& batch = ev->Get<TEvKqpCompute::TEvScanData>()->ArrowBatch) {
                    scannedRows += batch->num_rows();
                    ++batches;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        const auto alter = f.Execute(SetNotNull, true);
        runtime.SetObserverFunc(previous);
        UNIT_ASSERT_VALUES_EQUAL_C(alter.GetStatus(),
            position == ENullPosition::None ? EStatus::SUCCESS : EStatus::PRECONDITION_FAILED, alter.GetIssues().ToString());
        if (position == ENullPosition::First) {
            UNIT_ASSERT_VALUES_EQUAL(batches, 1);
            UNIT_ASSERT_LT(scannedRows, rowCount);
        } else {
            UNIT_ASSERT_GT(batches, 1);
            UNIT_ASSERT_VALUES_EQUAL(scannedRows, rowCount);
        }
        auto result = f.Execute("SELECT COUNT(*), COUNT(v) FROM `/Root/t`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(position == ENullPosition::None ? "[[100000u;100000u]]" : "[[100000u;99999u]]",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ValidationAcrossShards, ETableKind, EShardCount) {
        const ui32 shardCount = static_cast<ui32>(Arg<1>());
        TFixture f(Arg<0>(), true, shardCount);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TVector<TAutoPtr<IEventHandle>> requests;
        THashSet<ui64> shards;
        ui32 inFlight = 0;
        ui32 peakInFlight = 0;
        bool capture = true;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                if (shards.insert(ev->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record.GetTabletId()).second) {
                    peakInFlight = Max(peakInFlight, ++inFlight);
                    UNIT_ASSERT_LE(inFlight, 10);
                }
                if (capture) {
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionResponse::EventType) {
                UNIT_ASSERT_GT(inFlight, 0);
                --inFlight;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("first validation wave", [&] { return requests.size() == Min(shardCount, 10u); });
        UNIT_ASSERT(!alter.HasValue());
        capture = false;
        for (auto& request : requests) {
            runtime.Send(request.Release());
        }
        const auto result = runtime.WaitFuture(alter);
        runtime.SetObserverFunc(previous);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), shardCount);
        UNIT_ASSERT_VALUES_EQUAL(peakInFlight, Min(shardCount, 10u));
        UNIT_ASSERT_VALUES_EQUAL(inFlight, 0);
        f.CheckRows(R"([[1u;"one"]])");
    }

    Y_UNIT_TEST(WritePathsRespectConstraint, ETableKind, EWriteMode) {
        TFixture f(Arg<0>());
        const auto mode = Arg<1>();
        const auto checkWrite = [&](ui64 key, EWriteValue value, bool success) {
            const auto result = f.Write(mode, key, value);
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), success, result.GetIssues().ToString());
        };
        checkWrite(1, EWriteValue::Filled, true);
        checkWrite(2, EWriteValue::Null, true);
        checkWrite(3, EWriteValue::Missing, true);
        f.Success("DELETE FROM `/Root/t` WHERE k >= 2u;");

        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TVector<TAutoPtr<IEventHandle>> requests;
        bool capture = true;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (capture && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                requests.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("validation started", [&] { return !requests.empty(); });
        checkWrite(2, EWriteValue::Filled, true);
        checkWrite(3, EWriteValue::Null, false);
        checkWrite(4, EWriteValue::Missing, false);
        if (mode != EWriteMode::BulkUpsert) {
            f.Failure(TStringBuilder() << ToString(mode)
                << " INTO `/Root/t` (k, v) VALUES (4u, Just('filled'u));");
        }
        capture = false;
        for (auto& request : requests) {
            runtime.Send(request.Release());
        }
        const auto result = runtime.WaitFuture(alter);
        runtime.SetObserverFunc(previous);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        checkWrite(5, EWriteValue::Filled, true);
        checkWrite(6, EWriteValue::Null, false);
        checkWrite(7, EWriteValue::Missing, false);
        f.CheckRows(R"([[1u;"filled"];[2u;"filled"];[5u;"filled"]])");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v DROP NOT NULL;", true);
        checkWrite(6, EWriteValue::Null, true);
        checkWrite(7, EWriteValue::Missing, true);
    }

    Y_UNIT_TEST(WideTableProjectsOnlyCheckedColumn) {
        constexpr ui32 shardCount = 4;
        TFixture f(ETableKind::Column, true, shardCount);
        TStringBuilder create;
        create << "CREATE TABLE `/Root/wide` (k Uint64 NOT NULL, ";
        for (ui32 i = 0; i < 200; ++i) {
            create << "v" << i << " Utf8, ";
        }
        create << "PRIMARY KEY(k)) PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = " << shardCount << ");";
        f.Success(create, true);
        f.Success("UPSERT INTO `/Root/wide` (k, v199) VALUES (1u, 'filled'u);");

        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        ui32 scans = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType) {
                const auto& record = ev->Get<TEvDataShard::TEvKqpScan>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.ColumnTagsSize(), 1);
                ++scans;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        f.Success("ALTER TABLE `/Root/wide` ALTER COLUMN v199 SET NOT NULL;", true);
        runtime.SetObserverFunc(previous);
        UNIT_ASSERT_VALUES_EQUAL(scans, shardCount);
        f.Success("UPSERT INTO `/Root/wide` (k, v199) VALUES (2u, 'next'u);");
        f.Failure("UPSERT INTO `/Root/wide` (k, v199) VALUES (3u, NULL);");
    }

    Y_UNIT_TEST(FeatureFlag, ETableKind, EFeatureFlag) {
        const auto tableKind = Arg<0>();
        const bool enabled = Arg<1>() == EFeatureFlag::Enabled;
        TFixture f(tableKind, enabled);
        const auto result = f.Execute(SetNotNull, true);
        if (tableKind == ETableKind::Column && !enabled) {
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "SET NOT NULL for column tables is disabled");
            f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
        } else {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
        }
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, 'two'u);");
    }

    Y_UNIT_TEST(EmptyTableAndRepeatedRequest, ETableKind) {
        TFixture f(Arg<0>());
        f.Success(SetNotNull, true);
        f.Success(SetNotNull, true);
        f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
        f.Failure("UPSERT INTO `/Root/t` (k) VALUES (1u);");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'value');");
        f.CheckRows(R"([[1u;"value"]])");
    }

    Y_UNIT_TEST(ValidateExistingDataAndDropConstraint, ETableKind) {
        TFixture f(Arg<0>());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u), (2u, 'two'u);");
        f.Success(SetNotNull, true);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
        f.Failure("UPDATE `/Root/t` SET v = NULL WHERE k = 1u;");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v DROP NOT NULL;", true);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (3u, NULL);");
        f.CheckRows(R"([[1u;["one"]];[2u;["two"]];[3u;#]])");
    }

    Y_UNIT_TEST(RejectNullAndRetryAfterOverwriteAndDelete, ETableKind) {
        TFixture f(Arg<0>());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u), (2u, NULL), (3u, NULL);");
        auto result = f.Execute(SetNotNull, true);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (4u, NULL);");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, 'two'u);");
        f.Success("DELETE FROM `/Root/t` WHERE k >= 3u;");
        f.Success(SetNotNull, true);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
    }

    Y_UNIT_TEST(ColumnAddedAfterData, ETableKind) {
        TFixture f(Arg<0>());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        f.Success("ALTER TABLE `/Root/t` ADD COLUMN added Uint64;", true);
        f.Failure("ALTER TABLE `/Root/t` ALTER COLUMN added SET NOT NULL;", true);
        f.Success("UPSERT INTO `/Root/t` (k, added) VALUES (1u, 42u);");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN added SET NOT NULL;", true);
        auto result = f.Execute("SELECT added FROM `/Root/t`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[42u]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultipleColumnsAreAtomic, ETableKind) {
        TFixture f(Arg<0>());
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'one'u, NULL);");
        const TString alter = "ALTER TABLE `/Root/t` ALTER COLUMN v SET NOT NULL, ALTER COLUMN w SET NOT NULL;";
        f.Failure(alter, true);
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, NULL, NULL);");
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'one'u, 1u);");
        f.Success(alter, true);
        f.Failure("UPSERT INTO `/Root/t` (k, v, w) VALUES (2u, NULL, 2u);");
        f.Failure("UPSERT INTO `/Root/t` (k, v, w) VALUES (2u, 'two'u, NULL);");
    }

    Y_UNIT_TEST(DelayedWriteCannotUseOldNullableSchema, ETableKind) {
        TFixture f(Arg<0>());
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TAutoPtr<IEventHandle> delayed;
        bool capture = true;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (capture && ev->GetTypeRewrite() == NEvents::TDataEvents::TEvWrite::EventType &&
                ev->Get<NEvents::TDataEvents::TEvWrite>()->Record.OperationsSize()) {
                delayed = ev.Release();
                capture = false;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto write = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        runtime.WaitFor("write using old schema", [&] { return !!delayed; });
        f.Success(SetNotNull, true);
        runtime.Send(delayed.Release());
        const auto result = runtime.WaitFuture(write);
        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        runtime.SetObserverFunc(previous);
        f.CheckRows("[]");
    }

    Y_UNIT_TEST(WaitForAlreadyAcceptedWrite) {
        TFixture f;
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TAutoPtr<IEventHandle> delayed;
        bool capture = true;
        ui32 validationRequests = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                ++validationRequests;
            }
            if (capture && ev->GetTypeRewrite() == NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult::EventType) {
                delayed = ev.Release();
                capture = false;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto write = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
        runtime.WaitFor("accepted write before persistence", [&] { return !!delayed; });
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("validation started", [&] { return validationRequests != 0; });
        UNIT_ASSERT(!alter.HasValue());
        runtime.Send(delayed.Release());
        const auto writeResult = runtime.WaitFuture(write);
        UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        const auto alterResult = runtime.WaitFuture(alter);
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::PRECONDITION_FAILED, alterResult.GetIssues().ToString());
        runtime.SetObserverFunc(previous);
        f.CheckRows("[[1u;#]]");
    }

    Y_UNIT_TEST(CancelValidationRestoresNullableWrites, ETableKind) {
        TFixture f(Arg<0>());
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        ui64 schemeShardId = 0;
        ui64 operationId = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                const auto& record = ev->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record;
                schemeShardId = record.GetOwnerId();
                operationId = record.GetId();
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("validation started", [&] { return operationId != 0; });
        const auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSetColumnConstraint::TEvCancelRequest(0, "/Root", operationId));
        auto response = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSetColumnConstraint::TEvCancelResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        const auto result = runtime.WaitFuture(alter);
        UNIT_ASSERT(!result.IsSuccess());
        runtime.SetObserverFunc(previous);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
        f.Success("DROP TABLE `/Root/t`;", true);
    }

    Y_UNIT_TEST(CancelValidationStopsActiveScanner) {
        TFixture f(ETableKind::Column, true, 1);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        ui64 schemeShardId = 0;
        ui64 operationId = 0;
        TActorId validator;
        TActorId scanner;
        bool validatorStopped = false;
        bool scannerStopped = false;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                const auto& record = ev->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record;
                schemeShardId = record.GetOwnerId();
                operationId = record.GetId();
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType) {
                validator = ev->Sender;
            } else if (ev->GetTypeRewrite() == TEvKqpCompute::TEvScanDataAck::EventType && ev->Sender == validator) {
                scanner = ev->Recipient;
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType && ev->Recipient == validator) {
                validatorStopped = true;
            } else if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType && ev->Recipient == scanner) {
                scannerStopped = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("active validation scanner", [&] { return !!scanner || alter.HasValue(); });
        UNIT_ASSERT(scanner);
        const auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSetColumnConstraint::TEvCancelRequest(0, "/Root", operationId));
        auto response = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSetColumnConstraint::TEvCancelResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!runtime.WaitFuture(alter).IsSuccess());
        runtime.WaitFor("validation actors stopped", [&] { return validatorStopped && scannerStopped; });
        runtime.SetObserverFunc(previous);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, NULL);");
        f.Success("DROP TABLE `/Root/t`;", true);
    }

    Y_UNIT_TEST(WritesAndSchemaChangesDuringValidation, ETableKind, EReboot) {
        TFixture f(Arg<0>());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TVector<TAutoPtr<IEventHandle>> requests;
        bool capture = true;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (capture && ev->GetTypeRewrite() == TEvDataShard::TEvValidateRowConditionRequest::EventType) {
                requests.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto future = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).GetValueSync();
        });
        runtime.WaitFor("validation requests", [&] { return !requests.empty(); });
        const auto& record = requests.front()->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record;
        const ui64 schemeShardId = record.GetOwnerId();
        const ui64 shardId = record.GetTabletId();
        const ui64 operationId = record.GetId();
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, 'two'u);");
        f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (3u, NULL);");
        f.Failure("UPSERT INTO `/Root/t` (k, w) VALUES (1u, 7u);");
        f.CheckRows(R"([[1u;["one"]];[2u;["two"]]])");
        for (const auto* query : {"ALTER TABLE `/Root/t` DROP COLUMN v;", "DROP TABLE `/Root/t`;"}) {
            const auto result = f.Failure(query, true);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::OVERLOADED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "locked by tx");
        }
        const auto reboot = Arg<1>();
        if (reboot != EReboot::None) {
            // Recovery must finish even when admission is disabled.
            runtime.GetAppData().FeatureFlags.SetEnableColumnShardSetNotNull(false);
            if (reboot == EReboot::SchemeShard || reboot == EReboot::Both) {
                RebootTablet(runtime, schemeShardId, runtime.AllocateEdgeActor());
            }
            if (reboot == EReboot::TableShard || reboot == EReboot::Both) {
                RebootTablet(runtime, shardId, runtime.AllocateEdgeActor());
            }
        }
        capture = false;
        for (auto& request : requests) {
            runtime.Send(request.Release());
        }
        const auto result = runtime.WaitFuture(future);
        if (reboot == EReboot::SchemeShard || reboot == EReboot::Both) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAVAILABLE, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Connection to tablet was lost");
        } else {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        // SchemeShard recovery must complete the original operation even if its SQL connection was lost.
        const auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(operationId));
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSetColumnConstraint::TEvGetRequest("/Root", operationId));
        const auto response = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSetColumnConstraint::TEvGetResponse>(edge);
        const auto& status = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), Ydb::StatusIds::SUCCESS, status.ShortDebugString());
        UNIT_ASSERT_C(status.GetSetColumnConstraint().GetState() == Ydb::Table::SetNotNullState::STATE_DONE,
            status.ShortDebugString());
        runtime.SetObserverFunc(previous);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
    }
}

} // namespace NKikimr::NKqp
