#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
using namespace NYdb;

namespace {

TKikimrSettings MakeSettings(bool enabled = true) {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    settings.AppConfig.MutableFeatureFlags()->SetEnableSetColumnConstraint(true);
    settings.AppConfig.MutableFeatureFlags()->SetEnableColumnTableSetNotNull(enabled);
    return settings;
}

struct TFixture {
    TKikimrRunner Kikimr;
    NQuery::TQueryClient Client;

    explicit TFixture(bool enabled = true)
        : Kikimr(MakeSettings(enabled))
        , Client(Kikimr.RunCall([&] { return Kikimr.GetQueryClient(); }))
    {
        Success(R"(
            CREATE TABLE `/Root/t` (
                k Uint64 NOT NULL, v Utf8, w Uint64, PRIMARY KEY(k)
            ) PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = 4);
        )", true);
    }

    auto Execute(const TString& sql, bool ddl = false) {
        return Kikimr.RunCall([&] {
            return Client.ExecuteQuery(sql,
                ddl ? NQuery::TTxControl::NoTx() : NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        });
    }

    void Success(const TString& sql, bool ddl = false) {
        const auto result = Execute(sql, ddl);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void Failure(const TString& sql, bool ddl = false) {
        const auto result = Execute(sql, ddl);
        UNIT_ASSERT_C(!result.IsSuccess(), sql);
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
    Y_UNIT_TEST(TallTable) {
        TFixture f;
        f.Success(R"(
            $rows = ListMap(ListFromRange(0, 100000), ($x) -> { RETURN AsStruct($x AS item); });
            UPSERT INTO `/Root/t` (k, v)
            SELECT CAST(item AS Uint64), CAST(item AS Utf8) FROM AS_TABLE($rows);
        )");
        f.Success(SetNotNull, true);
        auto result = f.Execute("SELECT COUNT(*), COUNT(v) FROM `/Root/t`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[100000u;100000u]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(WideTableProjectsOnlyCheckedColumn) {
        TFixture f;
        TStringBuilder create;
        create << "CREATE TABLE `/Root/wide` (k Uint64 NOT NULL, ";
        for (ui32 i = 0; i < 200; ++i) {
            create << "v" << i << " Utf8, ";
        }
        create << "PRIMARY KEY(k)) PARTITION BY HASH(k) WITH (STORE = COLUMN, PARTITION_COUNT = 4);";
        f.Success(create, true);
        f.Success("UPSERT INTO `/Root/wide` (k, v199) VALUES (1u, 'filled');");

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
        UNIT_ASSERT_VALUES_EQUAL(scans, 4);
        f.Success("UPSERT INTO `/Root/wide` (k, v199) VALUES (2u, 'next');");
        f.Failure("UPSERT INTO `/Root/wide` (k, v199) VALUES (3u, NULL);");
    }

    Y_UNIT_TEST(FeatureFlag) {
        TFixture f(false);
        const auto result = f.Execute(SetNotNull, true);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "SET NOT NULL for column tables is disabled");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
    }

    Y_UNIT_TEST(EmptyTableAndRepeatedRequest) {
        TFixture f;
        f.Success(SetNotNull, true);
        f.Success(SetNotNull, true);
        f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (1u, NULL);");
        f.Failure("UPSERT INTO `/Root/t` (k) VALUES (1u);");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'value');");
        f.CheckRows(R"([[1u;"value"]])");
    }

    Y_UNIT_TEST(ValidateExistingDataAndDropConstraint) {
        TFixture f;
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'), (2u, 'two');");
        f.Success(SetNotNull, true);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
        f.Failure("UPDATE `/Root/t` SET v = NULL WHERE k = 1u;");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN v DROP NOT NULL;", true);
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (3u, NULL);");
        f.CheckRows(R"([[1u;["one"]];[2u;["two"]];[3u;#]])");
    }

    Y_UNIT_TEST(RejectNullAndRetryAfterOverwriteAndDelete) {
        TFixture f;
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one'), (2u, NULL), (3u, NULL);");
        auto result = f.Execute(SetNotNull, true);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (4u, NULL);");
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, 'two');");
        f.Success("DELETE FROM `/Root/t` WHERE k >= 3u;");
        f.Success(SetNotNull, true);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
    }

    Y_UNIT_TEST(ColumnMissingInOldPortions) {
        TFixture f;
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one');");
        f.Success("ALTER TABLE `/Root/t` ADD COLUMN added Uint64;", true);
        f.Failure("ALTER TABLE `/Root/t` ALTER COLUMN added SET NOT NULL;", true);
        f.Success("UPSERT INTO `/Root/t` (k, added) VALUES (1u, 42u);");
        f.Success("ALTER TABLE `/Root/t` ALTER COLUMN added SET NOT NULL;", true);
        auto result = f.Execute("SELECT added FROM `/Root/t`;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[42u]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(MultipleColumnsAreAtomic) {
        TFixture f;
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'one', NULL);");
        const TString alter = "ALTER TABLE `/Root/t` ALTER COLUMN v SET NOT NULL, ALTER COLUMN w SET NOT NULL;";
        f.Failure(alter, true);
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, NULL, NULL);");
        f.Success("UPSERT INTO `/Root/t` (k, v, w) VALUES (1u, 'one', 1u);");
        f.Success(alter, true);
        f.Failure("UPSERT INTO `/Root/t` (k, v, w) VALUES (2u, NULL, 2u);");
        f.Failure("UPSERT INTO `/Root/t` (k, v, w) VALUES (2u, 'two', NULL);");
    }

    Y_UNIT_TEST(DelayedWriteCannotUseOldNullableSchema) {
        TFixture f;
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

    Y_UNIT_TEST(CancelValidationRestoresNullableWrites) {
        TFixture f;
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

    Y_UNIT_TEST_TWIN(WritesAndSchemaChangesDuringValidation, Reboot) {
        TFixture f;
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (1u, 'one');");
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
            return f.Client.ExecuteQuery(SetNotNull, NQuery::TTxControl::NoTx()).GetValueSync();
        });
        runtime.WaitFor("validation requests", [&] { return !requests.empty(); });
        f.Success("UPSERT INTO `/Root/t` (k, v) VALUES (2u, 'two');");
        f.Failure("UPSERT INTO `/Root/t` (k, v) VALUES (3u, NULL);");
        f.Failure("UPSERT INTO `/Root/t` (k, w) VALUES (1u, 7u);");
        f.CheckRows(R"([[1u;["one"]];[2u;["two"]]])");
        f.Failure("ALTER TABLE `/Root/t` DROP COLUMN v;", true);
        f.Failure("DROP TABLE `/Root/t`;", true);
        if constexpr (Reboot) {
            const auto& record = requests.front()->Get<TEvDataShard::TEvValidateRowConditionRequest>()->Record;
            const ui64 schemeShardId = record.GetOwnerId();
            const ui64 columnShardId = record.GetTabletId();
            // Recovery must finish even when admission is disabled.
            runtime.GetAppData().FeatureFlags.SetEnableColumnTableSetNotNull(false);
            RebootTablet(runtime, schemeShardId, runtime.AllocateEdgeActor());
            RebootTablet(runtime, columnShardId, runtime.AllocateEdgeActor());
        }
        capture = false;
        for (auto& request : requests) {
            runtime.Send(request.Release());
        }
        const auto result = runtime.WaitFuture(future);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        runtime.SetObserverFunc(previous);
        f.CheckRows(R"([[1u;"one"];[2u;"two"]])");
    }
}

} // namespace NKikimr::NKqp
