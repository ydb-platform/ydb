#include "not_null_test_enums.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/portions/compacted.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <arrow/builder.h>
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

struct TValidationRequestIds {
    ui64 SchemeShardId;
    ui64 ShardId;
    ui64 OperationId;
};

std::optional<TValidationRequestIds> GetValidationRequestIds(IEventHandle& event, ETableKind tableKind) {
    const auto ids = [](const auto& record) -> TValidationRequestIds {
        return { record.GetOwnerId(), record.GetTabletId(), record.GetId() };
    };

    switch (event.GetTypeRewrite()) {
        case TEvDataShard::TEvValidateRowConditionRequest::EventType:
            UNIT_ASSERT(tableKind == ETableKind::Row);
            return ids(event.Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record);
        case TEvColumnShard::TEvValidateRowConditionRequest::EventType: {
            UNIT_ASSERT(tableKind == ETableKind::Column);
            const auto& record = event.Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetDatabaseName(), "/Root");
            return ids(record);
        }

        default:
            return std::nullopt;
    }
}

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
        const auto responseType = Arg<0>() == ETableKind::Column
            ? TEvColumnShard::TEvValidateRowConditionResponse::EventType
            : TEvDataShard::TEvValidateRowConditionResponse::EventType;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (const auto request = GetValidationRequestIds(*ev, Arg<0>())) {
                if (shards.insert(request->ShardId).second) {
                    peakInFlight = Max(peakInFlight, ++inFlight);
                    UNIT_ASSERT_LE(inFlight, 10);
                }

                if (capture) {
                    requests.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (ev->GetTypeRewrite() == responseType) {
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
            if (capture && GetValidationRequestIds(*ev, Arg<0>())) {
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

    Y_UNIT_TEST(DisableDeduplicationAfterSetNotNull, EScanReader) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TFixture f(ETableKind::Column, true, 1);
        f.Kikimr.GetTestServer().GetRuntime()->GetAppData().ColumnShardConfig.SetAlterObjectEnabled(true);
        const auto setDeduplication = [&](bool enabled) {
            f.Success(TStringBuilder() << "ALTER OBJECT `/Root/t` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, "
                << "DEDUPLICATION_ENABLED=`" << (enabled ? "true" : "false") << "`);", true);
        };

        const auto check = [&](const TString& sql, const TString& expected) {
            const auto result = f.Execute(sql);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(result.GetResultSet(0)));
        };

        setDeduplication(true);
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, NULL, 1u), (2u, 'old'u, NULL), (3u, 'keep'u, 3u);");
        f.Failure(SetNotNull, true);
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'one'u, 1u), (2u, 'two'u, 2u), (3u, 'new'u, 3u);");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v SET NOT NULL, ALTER COLUMN w SET NOT NULL;", true);
        check("SELECT COUNT(*) FROM `/Root/t`;", "[[3u]]");

        setDeduplication(false);
        const auto reader = Arg<0>();
        f.Success(TStringBuilder() << "ALTER OBJECT `/Root/t` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, "
            << "SCAN_READER_POLICY_NAME=`" << ToString(reader) << "`);", true);
        if (reader == EScanReader::PLAIN) {
            // PLAIN always merges versions by key, even when the duplicates filter is disabled.
            check("SELECT k, v, w FROM `/Root/t` ORDER BY k;", R"([[1u;"one";1u];[2u;"two";2u];[3u;"new";3u]])");
            check("SELECT COUNT(*) FROM `/Root/t`;", "[[3u]]");
            return;
        }

        check("SELECT k, v, w FROM `/Root/t` ORDER BY k, v;",
            R"([[1u;"one";1u];[2u;"two";2u];[3u;"keep";3u];[3u;"new";3u]])");
        check("SELECT k FROM `/Root/t` ORDER BY k;", "[[1u];[2u];[3u];[3u]]");
        check("SELECT COUNT(*), COUNT(v), COUNT(w) FROM `/Root/t`;", "[[4u;4u;4u]]");
        check("SELECT k FROM `/Root/t` WHERE k < 3u ORDER BY k LIMIT 2;", "[[1u];[2u]]");

        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v DROP NOT NULL;", true);
        check("SELECT COUNT(*), COUNT(v) FROM `/Root/t`;", "[[5u;4u]]");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN w DROP NOT NULL;", true);
        check("SELECT COUNT(*), COUNT(v), COUNT(w) FROM `/Root/t`;", "[[6u;5u;5u]]");
    }

    Y_UNIT_TEST(SnapshotReadAfterSetNotNull, ETableKind, ESnapshotRead) {
        const auto tableKind = Arg<0>();
        const bool project = Arg<1>() == ESnapshotRead::Projection;
        const TString query = project ? "SELECT k, v FROM `/Root/t` ORDER BY k;" : "SELECT COUNT(*) FROM `/Root/t` WHERE v IS NULL;";
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TFixture f(tableKind, true, 1);
        TStringBuilder anchor;
        anchor << "CREATE TABLE `/Root/anchor` (k Uint64 NOT NULL, PRIMARY KEY(k))";
        if (tableKind == ETableKind::Column) {
            anchor << " PARTITION BY HASH(k) WITH (STORE=COLUMN, PARTITION_COUNT=1)";
        }

        f.Success(anchor, true);
        f.Success("UPSERT INTO `/Root/anchor` (k) VALUES (1u);");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL), (2u, 'keep'u);");

        // Pin the data snapshot without resolving the schema of the target table.
        const auto beginSnapshot = [&] {
            return f.Kikimr.RunCall([&] {
                auto session = f.Client.GetSession().GetValueSync().GetSession();
                auto result = session.ExecuteQuery("SELECT k FROM `/Root/anchor`;",
                    NQuery::TTxControl::BeginTx(NQuery::TTxSettings::SnapshotRO())).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                CompareYson("[[1u]]", FormatResultSetYson(result.GetResultSet(0)));
                UNIT_ASSERT(result.GetTransaction() && result.GetTransaction()->IsActive());
                return *result.GetTransaction();
            });
        };

        const auto readSnapshot = [&](const NQuery::TTransaction& tx) {
            return f.Kikimr.RunCall([&] {
                return tx.GetSession().ExecuteQuery(query, NQuery::TTxControl::Tx(tx).CommitTx(),
                    NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Basic)
                        .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).GetValueSync();
            });
        };

        auto controlTx = beginSnapshot();
        auto oldTx = beginSnapshot();
        f.Success("DELETE FROM `/Root/t` WHERE k = 1u;");
        auto control = readSnapshot(controlTx);
        UNIT_ASSERT_C(control.IsSuccess(), control.GetIssues().ToString());
        CompareYson(project ? R"([[1u;#];[2u;["keep"]]])" : "[[1u]]", FormatResultSetYson(control.GetResultSet(0)));

        f.Success(SetNotNull, true);
        std::optional<NQuery::TTransaction> afterSet;
        if (project) {
            afterSet = beginSnapshot();
            auto& server = f.Kikimr.GetTestServer();
            auto& runtime = *server.GetRuntime();
            const auto schemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, server.GetSettings().Domain);
            RebootTablet(runtime, schemeShard, runtime.AllocateEdgeActor());
            f.Success("ALTER TABLE `/Root/t` ADD COLUMN extra Uint64;", true);
        }

        // Warm the query cache with a plan compiled against the new schema.
        auto fresh = f.Execute(query);
        UNIT_ASSERT_C(fresh.IsSuccess(), fresh.GetIssues().ToString());
        CompareYson(project ? R"([[2u;"keep"]])" : "[[0u]]", FormatResultSetYson(fresh.GetResultSet(0)));

        if (afterSet) {
            auto compatible = readSnapshot(*afterSet);
            UNIT_ASSERT_C(compatible.IsSuccess(), compatible.GetIssues().ToString());
            CompareYson(R"([[2u;"keep"]])", FormatResultSetYson(compatible.GetResultSet(0)));
            UNIT_ASSERT(compatible.GetStats() && compatible.GetStats()->GetCompilation());
            UNIT_ASSERT(compatible.GetStats()->GetCompilation()->IsFromCache());
        }

        auto historical = readSnapshot(oldTx);
        UNIT_ASSERT_VALUES_EQUAL_C(historical.GetStatus(), EStatus::ABORTED, historical.GetIssues().ToString());
        UNIT_ASSERT_C(HasIssue(historical.GetIssues(), NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH, [](const auto& issue) {
            return TStringBuf(issue.GetMessage()).Contains("SET NOT NULL");
        }), historical.GetIssues().ToString());
    }

    Y_UNIT_TEST(WriteAndReadAcrossPrimaryKeyConstraint) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TFixture f(ETableKind::Column, true, 1);
        f.Kikimr.GetTestServer().GetRuntime()->GetAppData().ColumnShardConfig.SetAllowNullableColumnsInPK(true);
        f.Success(R"(
            CREATE TABLE `/Root/keys` (k Int64 NOT NULL, subkey Int64, v Utf8, PRIMARY KEY(k, subkey))
            PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )", true);
        f.Success("UPSERT INTO `/Root/keys` (k, subkey, v) VALUES (1, 2, 'old'u), (1, 3, 'keep'u);");
        f.Success("ALTER TABLE `/Root/keys` ALTER COLUMN subkey SET NOT NULL;", true);
        f.Success("UPSERT INTO `/Root/keys` (k, subkey, v) VALUES (1, 1, 'new'u), (1, 2, 'replaced'u);");
        const auto result = f.Execute("SELECT k, subkey, v FROM `/Root/keys` ORDER BY k, subkey;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson(R"([[1;1;["new"]];[1;2;["replaced"]];[1;3;["keep"]]])", FormatResultSetYson(result.GetResultSet(0)));
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
        TFixture f(ETableKind::Column, true, 1);
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TAutoPtr<IEventHandle> delayed;
        bool capture = true;
        ui32 validationRequests = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                ++validationRequests;
            }

            if (capture && ev->GetTypeRewrite() == NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get<NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult>()->GetWriteStatus(), NKikimrProto::OK);
                delayed = ev.Release();
                capture = false;
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        auto write = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        runtime.WaitFor("accepted write before portion metadata is persisted", [&] { return !!delayed || write.HasValue(); });

        UNIT_ASSERT(delayed);
        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });

        runtime.WaitFor("validator retried while the accepted write is pending", [&] { return validationRequests >= 2 || alter.HasValue(); });

        UNIT_ASSERT(!alter.HasValue());
        runtime.Send(delayed.Release());
        const auto writeResult = runtime.WaitFuture(write);
        UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        const auto alterResult = runtime.WaitFuture(alter);
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::PRECONDITION_FAILED, alterResult.GetIssues().ToString());
        f.CheckRows("[[1u;#]]");
    }

    Y_UNIT_TEST(CancelValidationRestoresNullableWrites, ETableKind) {
        TFixture f(Arg<0>());
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        ui64 schemeShardId = 0;
        ui64 operationId = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (const auto request = GetValidationRequestIds(*ev, Arg<0>())) {
                schemeShardId = request->SchemeShardId;
                operationId = request->OperationId;
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
            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
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
            if (capture && GetValidationRequestIds(*ev, Arg<0>())) {
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

        const auto [schemeShardId, shardId, operationId] = GetValidationRequestIds(*requests.front(), Arg<0>()).value();
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

    Y_UNIT_TEST(FailedArrowWriteDoesNotBlockNextSetNotNull) {
        TFixture f(ETableKind::Column, true, 1);
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'filled'u, 7u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TVector<TAutoPtr<IEventHandle>> delayed;
        bool holdValidation = true;
        ui32 validationRequests = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                ++validationRequests;
                if (holdValidation) {
                    delayed.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        auto firstAlter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });

        runtime.WaitFor("first validation after write barrier", [&] { return !delayed.empty(); });

        arrow::UInt64Builder keyBuilder;
        arrow::StringBuilder valueBuilder;
        arrow::UInt64Builder otherBuilder;
        UNIT_ASSERT(keyBuilder.Append(2).ok());
        UNIT_ASSERT(valueBuilder.AppendNull().ok());
        UNIT_ASSERT(otherBuilder.Append(7).ok());
        std::shared_ptr<arrow::Array> keys;
        std::shared_ptr<arrow::Array> values;
        std::shared_ptr<arrow::Array> others;
        UNIT_ASSERT(keyBuilder.Finish(&keys).ok());
        UNIT_ASSERT(valueBuilder.Finish(&values).ok());
        UNIT_ASSERT(otherBuilder.Finish(&others).ok());
        auto batch = arrow::RecordBatch::Make(arrow::schema({
            arrow::field("k", arrow::uint64(), false),
            arrow::field("v", arrow::utf8()),
            arrow::field("w", arrow::uint64())
        }), 1, {keys, values, others});
        const TString data = NArrow::SerializeBatchNoCompression(batch);
        const TString schema = NArrow::SerializeSchema(*batch->schema());
        const auto write = f.Kikimr.RunCall([&] {
            return f.Kikimr.GetTableClient().BulkUpsert("/Root/t",
                NYdb::NTable::EDataFormat::ApacheArrow, data, schema).GetValueSync();
        });

        UNIT_ASSERT_C(!write.IsSuccess(), "Arrow NULL write under the guard unexpectedly succeeded");
        // This is the shard-side batch-builder diagnostic. It confirms that the
        // error happened after CreateWriteOperation, not in frontend cell checks.
        UNIT_ASSERT_C(write.GetIssues().ToString().find("cannot prepare incoming batch") != std::string::npos,
            write.GetIssues().ToString());

        holdValidation = false;
        for (auto& ev : delayed) {
            runtime.Send(ev.Release());
        }

        delayed.clear();
        const auto firstResult = runtime.WaitFuture(firstAlter);
        UNIT_ASSERT_C(firstResult.IsSuccess(), firstResult.GetIssues().ToString());

        // w has only committed non-NULL data. The rejected v write must not remain
        // in OperationsManager and block the next operation's old-schema drain.
        validationRequests = 0;
        auto secondAlter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(
                "ALTER TABLE `/Root/t` ALTER COLUMN w SET NOT NULL;",
                NQuery::TTxControl::NoTx()).GetValueSync();
        });

        // The buggy drain reschedules itself every 100 ms. Count those requests
        // instead of waiting for the SDK's multi-minute timeout.
        runtime.WaitFor("second ALTER completes without a leaked write", [&] {
            return secondAlter.HasValue() || validationRequests >= 20;
        });

        UNIT_ASSERT_C(secondAlter.HasValue(),
            "SET NOT NULL repeatedly drains an operation whose Arrow write already failed");
        const auto secondResult = runtime.WaitFuture(secondAlter);
        UNIT_ASSERT_C(secondResult.IsSuccess(), secondResult.GetIssues().ToString());
        f.CheckRows("[[1u;\"filled\"]]");
    }

    Y_UNIT_TEST(UnavailableSnapshotServiceReleasesWriteBarrier) {
        TFixture f(ETableKind::Column, true, 1);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        bool validating = false;
        bool undelivered = false;
        auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                validating = true;
            } else if (validating && ev->GetTypeRewrite() == NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot::EventType) {
                auto reply = IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown);
                UNIT_ASSERT(reply);
                runtime.Send(reply.Release());
                undelivered = true;
            }
        });

        const auto unavailable = f.Failure(SetNotNull, true);
        UNIT_ASSERT(undelivered);
        UNIT_ASSERT_VALUES_EQUAL_C(unavailable.GetStatus(), EStatus::UNAVAILABLE, unavailable.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(unavailable.GetIssues().ToString(), "Cannot acquire column constraint validation snapshot");
        observer.Remove();

        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, NULL);");
        const auto result = f.Failure(SetNotNull, true);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        f.CheckRows(R"([[1u;["one"]];[2u;#]])");
    }

    Y_UNIT_TEST(ValidationSnapshotCoversDrainedBulkUpsertWithPendingPlan) {
        TFixture f(ETableKind::Column, true, 2);
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TAutoPtr<IEventHandle> oldWriteResult;
        TAutoPtr<IEventHandle> validationScan;
        TVector<TAutoPtr<IEventHandle>> readSets;
        TActorId oldWriteShard;
        ui64 oldWriteTablet = 0;
        ui64 operationId = 0;
        ui64 blockerTxId = 0;
        ui64 blockerPlanStep = 0;
        ui64 scanStep = 0;
        ui64 scanTxId = 0;
        bool captureOldWrite = true;
        bool captureReadSets = false;
        bool captureScan = true;
        THashMap<ui64, ui64> plannedSteps;
        THashSet<ui64> acceptedSteps;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (captureOldWrite &&
                ev->GetTypeRewrite() == NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult::EventType) {
                oldWriteShard = ev->GetRecipientRewrite();
                oldWriteResult = ev.Release();
                captureOldWrite = false;
                return TTestActorRuntime::EEventAction::DROP;
            }

            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType &&
                ev->GetRecipientRewrite() == oldWriteShard) {
                const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
                oldWriteTablet = record.GetTabletId();
                operationId = record.GetId();
            }

            if (captureReadSets && ev->GetTypeRewrite() == TEvTxProcessing::TEvPlanStep::EventType) {
                const auto& record = ev->Get<TEvTxProcessing::TEvPlanStep>()->Record;
                if (record.GetTabletID() == oldWriteTablet) {
                    for (const auto& tx : record.GetTransactions()) {
                        plannedSteps[tx.GetTxId()] = record.GetStep();
                    }
                }
            }

            if (captureReadSets && ev->GetTypeRewrite() == TEvTxProcessing::TEvPlanStepAccepted::EventType) {
                const auto& record = ev->Get<TEvTxProcessing::TEvPlanStepAccepted>()->Record;
                if (record.GetTabletId() == oldWriteTablet) {
                    acceptedSteps.insert(record.GetStep());
                }
            }

            if (captureReadSets && ev->GetTypeRewrite() == TEvTxProcessing::TEvReadSet::EventType) {
                const auto& record = ev->Get<TEvTxProcessing::TEvReadSet>()->Record;
                if (record.GetTabletSource() == oldWriteTablet || record.GetTabletDest() == oldWriteTablet) {
                    if (!blockerTxId) {
                        blockerTxId = record.GetTxId();
                    }

                    UNIT_ASSERT_VALUES_EQUAL(blockerTxId, record.GetTxId());
                    readSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            if (captureScan && operationId &&
                ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType && ev->GetRecipientRewrite() == oldWriteShard &&
                ev->Get<TEvDataShard::TEvKqpScan>()->Record.GetTxId() == operationId) {
                const auto& record = ev->Get<TEvDataShard::TEvKqpScan>()->Record;
                scanStep = record.GetSnapshot().GetStep();
                scanTxId = record.GetSnapshot().GetTxId();
                validationScan = ev.Release();
                captureScan = false;
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        // ProtoValues is sufficient here: this write is accepted before any guard.
        // It must remain a public BulkUpsert/NoTxWrite, not a SQL transactional write.
        auto rows = TValueBuilder().BeginList().AddListItem().BeginStruct()
            .AddMember("k").Uint64(1)
            .AddMember("v").EmptyOptional(EPrimitiveType::Utf8)
            .AddMember("w").Uint64(7)
            .EndStruct().EndList().Build();
        auto bulk = f.Kikimr.RunInThreadPool([&] {
            return f.Kikimr.GetTableClient().BulkUpsert("/Root/t", std::move(rows)).GetValueSync();
        });

        runtime.WaitFor("pre-barrier BulkUpsert paused before persistence", [&] { return !!oldWriteResult; });

        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });

        runtime.WaitFor("write barrier installed on the old BulkUpsert shard", [&] { return operationId != 0; });

        UNIT_ASSERT(!alter.HasValue());

        // This writer uses the guard schema, hence its operations must not be part
        // of the old-schema drain. Use many disjoint keys to involve both shards;
        // the observed ReadSet/PlanStep below explicitly checks that premise.
        captureReadSets = true;
        auto blocker = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(R"(
                $rows = ListMap(ListFromRange(1000ul, 1256ul),
                    ($x) -> { RETURN AsStruct($x AS item); });
                UPSERT INTO `/Root/t` (k, v, w)
                SELECT item, 'filled'u, 7ul FROM AS_TABLE($rows);
            )", NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        runtime.WaitFor("post-barrier distributed transaction is planned and waits for readsets", [&] {
            const auto it = plannedSteps.find(blockerTxId);
            if (!readSets.empty() && it != plannedSteps.end() && acceptedSteps.contains(it->second)) {
                blockerPlanStep = it->second;
                return true;
            }

            return blocker.HasValue();
        });

        UNIT_ASSERT_C(blockerPlanStep != 0 && !blocker.HasValue(),
            "The test did not establish a pending distributed plan on the old-write shard");

        // The pending plan makes GetMaxReadVersion older than GetOutdatedSnapshot.
        // NoTxWrite commits at GetOutdatedSnapshot while this plan is still pending.
        runtime.Send(oldWriteResult.Release());
        const auto bulkResult = runtime.WaitFuture(bulk);
        UNIT_ASSERT_C(bulkResult.IsSuccess(), bulkResult.GetIssues().ToString());
        runtime.WaitFor("validator fixes its post-drain snapshot", [&] { return !!validationScan; });

        UNIT_ASSERT(!blocker.HasValue());

        // Release the blocker before dispatching the scan. Its selected snapshot
        // cannot change now: baseline still reads before the drained NULL commit,
        // even though the pending plan has finished. No time-based race is needed.
        captureReadSets = false;
        for (auto& ev : readSets) {
            runtime.Send(ev.Release());
        }

        readSets.clear();
        const auto blockerResult = runtime.WaitFuture(blocker);
        UNIT_ASSERT_C(blockerResult.IsSuccess(), blockerResult.GetIssues().ToString());
        runtime.Send(validationScan.Release());
        const auto alterResult = runtime.WaitFuture(alter);
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::PRECONDITION_FAILED,
            TStringBuilder() << "blocker plan=" << blockerPlanStep << ':' << blockerTxId
                << ", validator snapshot=" << scanStep << ':' << scanTxId
                << ", issues=" << alterResult.GetIssues().ToString());

        auto nullRows = f.Execute("SELECT COUNT(*) FROM `/Root/t` WHERE v IS NULL;");
        UNIT_ASSERT_C(nullRows.IsSuccess(), nullRows.GetIssues().ToString());
        CompareYson("[[1u]]", FormatResultSetYson(nullRows.GetResultSet(0)));
    }

    Y_UNIT_TEST(OldValidationRetryCannotReplaceRecoveredValidator) {
        TFixture f(ETableKind::Column, true, 1);
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        TAutoPtr<IEventHandle> delayedWrite;
        TAutoPtr<IEventHandle> delayedRetry;
        TAutoPtr<IEventHandle> recoveredRequest;
        TAutoPtr<IEventHandle> delayedAck;
        IEventHandle* scheduledRetry = nullptr;
        bool retryScheduled = false;
        bool captureWrite = true;
        bool captureRecoveredRequest = true;
        bool captureAck = true;
        bool currentValidatorPoisoned = false;
        bool currentResponseReceived = false;
        ui64 operationId = 0;
        ui64 schemeShardId = 0;
        ui64 oldGeneration = 0;
        TActorId columnShard;
        TActorId currentValidator;

        auto previousScheduled = runtime.SetScheduledEventFilter(
            [](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) { return false; });

        runtime.SetScheduledEventFilter(
            [&, previousScheduled](TTestActorRuntimeBase& r, TAutoPtr<IEventHandle>& ev,
                TDuration delay, TInstant& deadline) {
                if (!retryScheduled && ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                    const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
                    if (record.GetId() == operationId && record.GetSeqNoGeneration() == oldGeneration) {
                        // Observe the actual drain timer, then capture it when it is delivered.
                        // Keep ownership here: the scheduling code still accesses the handle.
                        scheduledRetry = ev.Get();
                        retryScheduled = true;
                        return false;
                    }
                }

                return previousScheduled(r, ev, delay, deadline);
            });

        Y_DEFER { runtime.SetScheduledEventFilter(previousScheduled); };

        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (captureWrite && ev->GetTypeRewrite() == NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult::EventType) {
                captureWrite = false;
                delayedWrite = ev.Release();
                return TTestActorRuntime::EEventAction::DROP;
            }

            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
                if (!operationId) {
                    operationId = record.GetId();
                    schemeShardId = record.GetOwnerId();
                    oldGeneration = record.GetSeqNoGeneration();
                }

                if (ev.Get() == scheduledRetry) {
                    columnShard = ev->GetRecipientRewrite();
                    scheduledRetry = nullptr;
                    delayedRetry = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (captureRecoveredRequest && record.GetId() == operationId && record.GetSeqNoGeneration() > oldGeneration) {
                    captureRecoveredRequest = false;
                    recoveredRequest = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType &&
                ev->Get<TEvDataShard::TEvKqpScan>()->Record.GetTxId() == operationId && !currentValidator) {
                currentValidator = ev->Sender;
            } else if (captureAck && ev->GetTypeRewrite() == TEvKqpCompute::TEvScanDataAck::EventType &&
                ev->Sender == currentValidator) {
                captureAck = false;
                delayedAck = ev.Release();
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType && ev->Recipient == currentValidator && !currentResponseReceived) {
                currentValidatorPoisoned = true;
            } else if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionResponse::EventType &&
                ev->Sender == currentValidator) {
                currentResponseReceived = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        auto write = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);",
                NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });

        runtime.WaitFor("accepted pre-barrier write", [&] { return !!delayedWrite; });

        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).GetValueSync();
        });

        runtime.WaitFor("old generation drain retry", [&] { return !!delayedRetry; });

        UNIT_ASSERT(!currentValidator);

        RebootTablet(runtime, schemeShardId, runtime.AllocateEdgeActor());
        runtime.WaitFor("recovered validation request", [&] { return !!recoveredRequest; });

        runtime.Send(delayedWrite.Release());
        const auto writeResult = runtime.WaitFuture(write);
        UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

        runtime.Send(recoveredRequest.Release());
        runtime.WaitFor("recovered validator scanner", [&] { return !!delayedAck; });

        runtime.Send(delayedRetry.Release());

        // An immediately rejected request is a mailbox barrier: process the old timer
        // before releasing V2's ACK, without sleeps or fabricating a successful result.
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(columnShard, edge, new TEvColumnShard::TEvValidateRowConditionRequest));
        auto barrier = runtime.GrabEdgeEventRethrow<TEvColumnShard::TEvValidateRowConditionResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(barrier->Get()->Record.GetStatus(), NKikimrSetColumnConstraint::BAD_REQUEST);

        runtime.Send(delayedAck.Release());
        runtime.WaitFor("recovered validation response or stale replacement", [&] {
            return currentResponseReceived || currentValidatorPoisoned;
        });

        UNIT_ASSERT_C(!currentValidatorPoisoned, "The old SchemeShard drain timer replaced the recovered validator");
        UNIT_ASSERT(currentResponseReceived);

        const auto alterResult = runtime.WaitFuture(alter);
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::UNAVAILABLE, alterResult.GetIssues().ToString());
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(operationId));
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSetColumnConstraint::TEvGetRequest("/Root", operationId));
        const auto status = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSetColumnConstraint::TEvGetResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(status->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_C(status->Get()->Record.GetSetColumnConstraint().GetState() == Ydb::Table::SetNotNullState::STATE_DONE,
            status->Get()->Record.ShortDebugString());

        runtime.SetObserverFunc(previous);
        runtime.SetScheduledEventFilter(previousScheduled);
        f.CheckRows(R"([[1u;"one"]])");
        f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (2u, NULL);");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v DROP NOT NULL;", true);
        f.Success(SetNotNull, true);
    }

    Y_UNIT_TEST(CancelBeforeScanInitReleasesScanner) {
        TFixture f(ETableKind::Column, true, 1);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'u);");
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        ui64 schemeShardId = 0;
        ui64 operationId = 0;
        TActorId validator;
        TActorId scanner;
        TAutoPtr<IEventHandle> delayedInit;
        bool captureInit = true;
        bool validatorFinished = false;
        bool initUndelivered = false;
        bool readFinished = false;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
                schemeShardId = record.GetOwnerId();
                operationId = record.GetId();
            } else if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType) {
                validator = ev->Sender;
            } else if (captureInit && ev->GetTypeRewrite() == TEvKqpCompute::TEvScanInitActor::EventType &&
                ev->Recipient == validator) {
                captureInit = false;
                scanner = ev->Sender;
                delayedInit = ev.Release();
                return TTestActorRuntime::EEventAction::DROP;
            } else if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvColumnConstraintValidationFinished::EventType &&
                ev->Sender == validator) {
                validatorFinished = true;
            } else if (ev->GetTypeRewrite() == TEvents::TEvUndelivered::EventType && ev->Recipient == scanner &&
                ev->Get<TEvents::TEvUndelivered>()->SourceType == TEvKqpCompute::TEvScanInitActor::EventType) {
                initUndelivered = true;
            } else if (ev->GetTypeRewrite() == NColumnShard::TEvPrivate::TEvReadFinished::EventType && ev->Sender == scanner) {
                readFinished = true;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });

        runtime.WaitFor("scanner init in flight", [&] { return !!delayedInit; });

        const auto edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(schemeShardId, edge,
            new NSchemeShard::TEvSetColumnConstraint::TEvCancelRequest(0, "/Root", operationId));
        const auto cancel = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSetColumnConstraint::TEvCancelResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(cancel->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        runtime.WaitFor("validator stopped before ScanInit", [&] { return validatorFinished; });

        // Preserve FlagTrackDelivery: the dead destination must produce Undelivered.
        runtime.Send(delayedInit.Release());
        runtime.WaitFor("scanner releases its read", [&] { return readFinished; });

        UNIT_ASSERT(initUndelivered);
        UNIT_ASSERT(!runtime.WaitFuture(alter).IsSuccess());
        runtime.SetObserverFunc(previous);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, NULL);");
        f.Success("DROP TABLE `/Root/t`;", true);
    }

    enum class ERecoveredData {
        BulkCommit,
        CompactedNull,
        CleanedAfterRepair,
    };

    void CheckRecoveredValidation(ERecoveredData data) {
        using EBackground = NYDBTest::ICSController::EBackground;
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->DisableBackground(EBackground::Compaction);
        TFixture f(ETableKind::Column, true, 1);
        auto& runtime = *f.Kikimr.GetTestServer().GetRuntime();
        if (data != ERecoveredData::BulkCommit) {
            controller->DisableBackground(EBackground::Cleanup);
            controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            controller->SetOverrideCompactionActualizationLag(TDuration::Zero());
            controller->SetOverrideLagForCompactionBeforeTierings(TDuration::Zero());
            runtime.GetAppData().FeatureFlags.SetEnableSnapshotsLocking(false);
            // The delayed write will overwrite this key in another portion.
            f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'seed'u, 7u);");
        }

        TAutoPtr<IEventHandle> oldWriteResult;
        TAutoPtr<IEventHandle> firstValidation;
        TAutoPtr<IEventHandle> recoveredScan;
        TVector<TAutoPtr<IEventHandle>> timecastResults;
        TVector<TAutoPtr<IEventHandle>> timecastNotifications;
        bool captureWrite = true;
        bool afterReboot = false;
        bool holdTimecast = true;
        ui64 shardId = 0;
        ui64 operationId = 0;
        const auto previous = runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (captureWrite && ev->GetTypeRewrite() == NColumnShard::NPrivateEvents::NWrite::TEvWritePortionResult::EventType) {
                captureWrite = false;
                oldWriteResult = ev.Release();
                return TTestActorRuntime::EEventAction::DROP;
            }

            if (!afterReboot && ev->GetTypeRewrite() == TEvColumnShard::TEvValidateRowConditionRequest::EventType) {
                if (!firstValidation) {
                    const auto& record = ev->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
                    shardId = record.GetTabletId();
                    operationId = record.GetId();
                    firstValidation = ev.Release();
                }

                return TTestActorRuntime::EEventAction::DROP;
            }

            if (afterReboot && holdTimecast) {
                if (ev->GetTypeRewrite() == TEvMediatorTimecast::TEvRegisterTabletResult::EventType &&
                    ev->Get<TEvMediatorTimecast::TEvRegisterTabletResult>()->TabletId == shardId) {
                    timecastResults.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (ev->GetTypeRewrite() == TEvMediatorTimecast::TEvNotifyPlanStep::EventType &&
                    ev->Get<TEvMediatorTimecast::TEvNotifyPlanStep>()->TabletId == shardId) {
                    timecastNotifications.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }

                if (!recoveredScan && ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType &&
                    ev->Get<TEvDataShard::TEvKqpScan>()->Record.GetTxId() == operationId) {
                    recoveredScan = ev.Release();
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Y_DEFER { runtime.SetObserverFunc(previous); };

        auto rows = TValueBuilder().BeginList().AddListItem().BeginStruct()
            .AddMember("k").Uint64(1)
            .AddMember("v").EmptyOptional(EPrimitiveType::Utf8)
            .AddMember("w").Uint64(7)
            .EndStruct().EndList().Build();
        auto bulk = f.Kikimr.RunInThreadPool([&] {
            return f.Kikimr.GetTableClient().BulkUpsert("/Root/t", std::move(rows)).GetValueSync();
        });

        runtime.WaitFor("pre-guard BulkUpsert accepted", [&] { return !!oldWriteResult; });

        auto alter = f.Kikimr.RunInThreadPool([&] {
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });

        runtime.WaitFor("guard installed before old BulkUpsert commits", [&] { return !!firstValidation; });

        const auto guardSnapshot = controller->GetTheOnlyShard()->GetLastPlannedSnapshot();

        const auto advanceTimecastPast = [&](ui64 step) {
            runtime.Send(new IEventHandle(MakeMediatorTimecastProxyID(), controller->GetTheOnlyShard()->SelfId(),
                new TEvMediatorTimecast::TEvWaitPlanStep(shardId, step + 1)));
            runtime.WaitFor("mediator frontier passes requested step", [&] {
                return controller->GetTheOnlyShard()->GetOutdatedSnapshot().GetPlanStep() > step;
            }, TDuration::Seconds(30));
        };

        advanceTimecastPast(guardSnapshot.GetPlanStep());
        runtime.Send(oldWriteResult.Release());
        const auto bulkResult = runtime.WaitFuture(bulk);
        UNIT_ASSERT_C(bulkResult.IsSuccess(), bulkResult.GetIssues().ToString());

        const auto portions = [&]() -> const auto& {
            const auto& tables = controller->GetTheOnlyShard()->GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            return tables.begin()->second->GetPortions();
        };

        NOlap::TSnapshot committed = NOlap::TSnapshot::Zero();
        for (const auto& [id, portion] : portions()) {
            UNIT_ASSERT(!portion->IsAborted());
            committed = std::max(committed, portion->RecordSnapshotMax());
        }

        UNIT_ASSERT_C(committed.GetPlanStep() > guardSnapshot.GetPlanStep(), "BulkUpsert must commit after the guard");

        if (data != ERecoveredData::BulkCommit) {
            if (data == ERecoveredData::CleanedAfterRepair) {
                advanceTimecastPast(committed.GetPlanStep());
                const auto repair = f.Write(EWriteMode::BulkUpsert, 1, EWriteValue::Filled);
                UNIT_ASSERT_C(repair.IsSuccess(), repair.GetIssues().ToString());
            }

            THashSet<ui64> originalPortionIds;
            for (const auto& [id, portion] : portions()) {
                UNIT_ASSERT(!portion->HasRemoveSnapshot());
                originalPortionIds.insert(id);
            }

            UNIT_ASSERT(originalPortionIds.size() >= 2);

            const auto& request = firstValidation->Get<TEvColumnShard::TEvValidateRowConditionRequest>()->Record;
            const auto sender = runtime.AllocateEdgeActor();
            const auto wakeup = [&] {
                runtime.Send(new IEventHandle(controller->GetTheOnlyShard()->SelfId(), sender,
                    new NColumnShard::TEvPrivate::TEvPeriodicWakeup(true)));
            };

            const auto compactionsBefore = controller->GetCompactionFinishedCounter().Val();
            controller->EnableBackground(EBackground::Compaction);
            runtime.Send(new IEventHandle(controller->GetTheOnlyShard()->SelfId(), sender,
                new TEvDataShard::TEvCompactTable(request.GetOwnerId(), request.GetPathId())));
            wakeup();
            NOlap::TSnapshot compactedAppeared = NOlap::TSnapshot::Zero();
            runtime.WaitFor("all source portions replaced by compacted output", [&] {
                size_t removed = 0;
                compactedAppeared = NOlap::TSnapshot::Zero();
                for (const auto& [id, portion] : portions()) {
                    if (originalPortionIds.contains(id) && portion->HasRemoveSnapshot()) {
                        ++removed;
                    }

                    if (!portion->HasRemoveSnapshot() && portion->GetPortionType() == NOlap::EPortionType::Compacted) {
                        const auto& compacted = static_cast<const NOlap::TCompactedPortionInfo&>(*portion);
                        compactedAppeared = std::max(compactedAppeared, compacted.GetAppeared());
                    }
                }

                return removed == originalPortionIds.size() && compactedAppeared > committed &&
                    controller->GetCompactionFinishedCounter().Val() > compactionsBefore;
            }, TDuration::Seconds(30));

            controller->DisableBackground(EBackground::Compaction);

            // Wait until ordinary reads can see the replacement portion. Compaction can
            // publish it at a step ahead of the last mediator update on the tablet.
            if (data == ERecoveredData::CleanedAfterRepair) {
                const auto maxReadStaleness = TDuration::Seconds(1);
                controller->SetOverrideMaxReadStaleness(maxReadStaleness);
                advanceTimecastPast(compactedAppeared.GetPlanStep() + maxReadStaleness.MilliSeconds());
                const auto erasedBefore = controller->GetPortionsErasedCounter().Val();
                controller->EnableBackground(EBackground::Cleanup);
                wakeup();
                runtime.WaitFor("original portion metadata physically erased", [&] {
                    const auto& retained = portions();
                    for (const auto id : originalPortionIds) {
                        if (retained.contains(id)) {
                            return false;
                        }
                    }

                    return controller->GetPortionsErasedCounter().Val() >= erasedBefore + static_cast<intptr_t>(originalPortionIds.size());
                }, TDuration::Seconds(30));

                controller->DisableBackground(EBackground::Cleanup);
                controller->SetOverrideMaxReadStaleness(TDuration::Days(1));
            } else {
                advanceTimecastPast(compactedAppeared.GetPlanStep());
            }
        }

        UNIT_ASSERT(controller->GetTheOnlyShard()->GetLastPlannedSnapshot() == guardSnapshot);

        // SchemeShard retries the original ALTER after the tablet-pipe reconnect.
        afterReboot = true;
        RebootTablet(runtime, shardId, runtime.AllocateEdgeActor());
        runtime.WaitFor("recovered scan chooses its snapshot before local timecast registration", [&] {
            return (recoveredScan && !timecastResults.empty()) || alter.HasValue();
        }, TDuration::Seconds(30));

        UNIT_ASSERT(recoveredScan && !timecastResults.empty());
        const auto& proto = recoveredScan->Get<TEvDataShard::TEvKqpScan>()->Record.GetSnapshot();
        const NOlap::TSnapshot selected(proto.GetStep(), proto.GetTxId());
        UNIT_ASSERT_C(selected >= committed, "Recovered validation snapshot must cover the drained write");

        holdTimecast = false;
        for (auto& ev : timecastResults) {
            runtime.Send(ev.Release());
        }

        for (auto& ev : timecastNotifications) {
            runtime.Send(ev.Release());
        }

        runtime.Send(recoveredScan.Release());
        const auto result = runtime.WaitFuture(alter);
        if (data == ERecoveredData::CleanedAfterRepair) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            f.CheckRows(R"([[1u;"filled"]])");
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(RecoveredValidationCoversBulkCommitBeforeTimecastRegistration) {
        CheckRecoveredValidation(ERecoveredData::BulkCommit);
    }

    Y_UNIT_TEST(RecoveredValidationCoversCompactedNullBeforeTimecastRegistration) {
        CheckRecoveredValidation(ERecoveredData::CompactedNull);
    }

    Y_UNIT_TEST(RecoveredValidationCoversPhysicalCleanupBeforeTimecastRegistration) {
        CheckRecoveredValidation(ERecoveredData::CleanedAfterRepair);
    }
}

} // namespace NKikimr::NKqp
