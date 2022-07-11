#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/kqp_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/grpc/client/grpc_client_low.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

class KqpForceNewEngine : public TTestBase {
public:
    void SetUp() override {
        FailForcedNewEngineCompilationForTests(false);
        FailForcedNewEngineExecutionForTests(false);

        TVector<NKikimrKqp::TKqpSetting> settings;
        NKikimrKqp::TKqpSetting setting;
        setting.SetName("_KqpForceNewEngine");
        setting.SetValue("false");
        settings.push_back(setting);

        TKikimrRunner kikimr(settings);

        Kikimr.reset(new TKikimrRunner(settings));
        Counters = Kikimr->GetTestServer().GetRuntime()->GetAppData(0).Counters;
        KqpCounters.reset(new TKqpCounters(Counters));
    }

    void TearDown() override {
        FailForcedNewEngineCompilationForTests(false);
        FailForcedNewEngineExecutionForTests(false);
    }

    TSession Session() {
        return Kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
    }

    void ForceNewEngine(ui32 percent, ui32 level) {
        NGrpc::TGRpcClientLow grpcClient;
        auto grpcContext = grpcClient.CreateContext();

        NGrpc::TGRpcClientConfig grpcConfig(Kikimr->GetEndpoint());
        auto grpc = grpcClient.CreateGRpcServiceConnection<NKikimrClient::TGRpcServer>(grpcConfig);

        NKikimrClient::TConsoleRequest request;
        auto* action = request.MutableConfigureRequest()->MutableActions()->Add();
        auto* configItem = action->MutableAddConfigItem()->MutableConfigItem();
        configItem->SetKind(NKikimrConsole::TConfigItem::TableServiceConfigItem);

        configItem->MutableConfig()->MutableTableServiceConfig()->SetForceNewEnginePercent(percent);
        configItem->MutableConfig()->MutableTableServiceConfig()->SetForceNewEngineLevel(level);

        std::atomic<int> done = 0;
        grpc->DoRequest<NKikimrClient::TConsoleRequest, NKikimrClient::TConsoleResponse>(
            request,
            [&done](NGrpc::TGrpcStatus&& status, NKikimrClient::TConsoleResponse&& response) {
                if (status.Ok()) {
                    if (response.GetStatus().code() != Ydb::StatusIds::SUCCESS) {
                        done = 3;
                    } else if (response.GetConfigureResponse().GetStatus().code() != Ydb::StatusIds::SUCCESS) {
                        done = 4;
                    } else {
                        done = 1;
                    }
                } else {
                    Cerr << "status: " << status.Msg << ", " << status.InternalError << ", " << status.GRpcStatusCode << Endl;
                    Cerr << response.DebugString() << Endl;
                    done = 2;
                }
            },
            &NKikimrClient::TGRpcServer::Stub::AsyncConsoleRequest,
            {},
            grpcContext.get());

        while (done.load() == 0) {
            ::Sleep(TDuration::Seconds(1));
        }
        grpcContext.reset();
        grpcClient.Stop(true);

        UNIT_ASSERT_VALUES_EQUAL(done.load(), 1);
    }

    void TestNotInteractiveReadOnlyTx(ui32 level, bool withSqlIn = false) {
        auto session = Session();

        auto test = [&](ui32 count) {
            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);

            for (ui32 i = 0; i < count; ++i) {
                auto query = withSqlIn
                    ? R"(
                        --!syntax_v1
                        DECLARE $values AS List<String>;
                        SELECT * FROM `/Root/TwoShard` WHERE Value1 IN $values
                      )"
                    : R"(
                        SELECT * FROM `/Root/TwoShard` WHERE Key = 1
                      )";
                auto params = TParamsBuilder()
                    .AddParam("$values").BeginList().AddListItem().String("One").EndList().Build().Build();
                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
        };

        {
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(50, level);
            test(20);
            bool diff = KqpCounters->NewEngineForcedQueryCount->Val() * KqpCounters->NewEngineCompatibleQueryCount->Val() != 0;
            if (level == 0 && withSqlIn) {
                UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
                UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
            } else {
                UNIT_ASSERT_C(diff, "forced: " << KqpCounters->NewEngineForcedQueryCount->Val()
                    << ", compatible: " << KqpCounters->NewEngineCompatibleQueryCount->Val());
            }
        }

        {
            ForceNewEngine(100, level);
            test(2);
            if (level == 0 && withSqlIn) {
                UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(2, KqpCounters->NewEngineForcedQueryCount->Val());
            }
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(0, level);
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }
    }

    void TestNotInteractiveWriteOnlyTx(ui32 level) {
        auto session = Session();

        auto test = [&](ui32 count) {
            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);

            for (ui32 i = 0; i < count; ++i) {
                auto result = session.ExecuteDataQuery(R"(
                    REPLACE INTO `/Root/TwoShard` (Key, Value1) VALUES (1, "OneOne")
                )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
        };

        {
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(100, level);
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(level == 3 ? 2 : 0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }
    }

    void TestNotInteractiveReadOnlyTxFailedNECompilation(ui32 level) {
        auto session = Session();

        auto test = [&]() {
            KqpCounters->ForceNewEngineCompileErrors->Set(0);
            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);

            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/KeyValue` LIMIT 1
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        {
            ForceNewEngine(100, level);
            FailForcedNewEngineCompilationForTests();
            test();
            UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->ForceNewEngineCompileErrors->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }
    }

    void TestNotInteractiveReadOnlyTxFallback(ui32 level) {
        auto session = Session();

        auto test = [&]() {
            KqpCounters->ForceNewEngineCompileErrors->Set(0);
            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);

            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/KeyValue` LIMIT 1
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            if (level == 0 || level == 1) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            }
        };

        {
            ForceNewEngine(100, level);
            FailForcedNewEngineExecutionForTests();
            test();
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val()); // failed request is not counted
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(level == 0 || level == 1 ? 1 : 0, GetServiceCounters(Counters, "kqp")->GetCounter("Requests/OldEngineFallback", true)->Val());
            UNIT_ASSERT_VALUES_EQUAL(level == 0 || level == 1 ? 0 : 1, GetServiceCounters(Counters, "kqp")->GetCounter("Requests/ForceNewEngineExecError", true)->Val());
        }
    }

    void TestInteractiveReadWriteEx(ui32 level, bool addBeginTx, /* bool prependWrite, */ bool appendRead, bool addCommitTx) {
        UNIT_ASSERT(level == 2);

        auto session = Session();

        auto test = [&](ui32 count) {
            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);

            for (ui32 i = 0; i < count; ++i) {
                auto txControl = TTxControl::BeginTx();

                if (addBeginTx) {
                    auto tx = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync().GetTransaction();
                    UNIT_ASSERT(tx.IsActive());

                    txControl = TTxControl::Tx(tx);
                }

                // TODO: prependWrite

                auto result = session.ExecuteDataQuery(R"(
                    SELECT * FROM `/Root/KeyValue` LIMIT 1
                )", txControl).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                txControl = TTxControl::Tx(*result.GetTransaction());

                if (appendRead || addCommitTx) {
                    // do nothing
                } else {
                    txControl = txControl.CommitTx();
                }

                result = session.ExecuteDataQuery(Sprintf(R"(
                    UPDATE `/Root/TwoShard` SET Value2 = %d WHERE Key = 1
                )", i), txControl).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                if (appendRead) {
                    txControl = TTxControl::Tx(*result.GetTransaction());
                    if (!addCommitTx) {
                        txControl = txControl.CommitTx();
                    }

                    result = session.ExecuteDataQuery(R"(
                        SELECT * FROM `/Root/KeyValue` LIMIT 2
                    )", txControl).ExtractValueSync();
                    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

                    if (addCommitTx) {
                        auto txResult = result.GetTransaction()->Commit().ExtractValueSync();
                        UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
                    }
                } else {
                    if (addCommitTx) {
                        auto txResult = result.GetTransaction()->Commit().ExtractValueSync();
                        UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
                    }
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
        };

        {
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(50, level);
            test(20);
            bool diff = KqpCounters->NewEngineForcedQueryCount->Val() * KqpCounters->NewEngineCompatibleQueryCount->Val() != 0;
            UNIT_ASSERT_C(diff, "forced: " << KqpCounters->NewEngineForcedQueryCount->Val()
                << ", compatible: " << KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(100, level);
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(2 * (appendRead && addCommitTx ? 2 : 1), KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }

        {
            ForceNewEngine(0, level);
            test(2);
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
            UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
        }
    }


    UNIT_TEST_SUITE(KqpForceNewEngine);
        UNIT_TEST(Level0_NotInteractiveReadOnly);
        UNIT_TEST(Level0_NotInteractiveReadOnlySqlIn);
        UNIT_TEST(Level0_NotInteractiveWriteOnly);
        UNIT_TEST(Level0_NotInteractiveReadWrite);
        UNIT_TEST(Level0_InteractiveReadOnly);
        UNIT_TEST(Level0_CompilationFailure);
        UNIT_TEST(Level0_Fallback);

        UNIT_TEST(Level1_NotInteractiveReadOnly);
        UNIT_TEST(Level1_NotInteractiveReadOnlySqlIn);
        UNIT_TEST(Level1_NotInteractiveWriteOnly);
        UNIT_TEST(Level1_NotInteractiveReadWrite);
        UNIT_TEST(Level1_InteractiveReadOnly);
        UNIT_TEST(Level1_CompilationFailure);
        UNIT_TEST(Level1_Fallback);

        UNIT_TEST(Level2_NotInteractiveReadOnly);
        UNIT_TEST(Level2_NotInteractiveWriteOnly);
        UNIT_TEST(Level2_InteractiveReadOnly);
        UNIT_TEST(Level2_InteractiveReadWrite);
        UNIT_TEST(Level2_InteractiveBeginReadWrite);
        UNIT_TEST(Level2_InteractiveReadWriteCommit);
        UNIT_TEST(Level2_InteractiveBeginReadWriteCommit);
        UNIT_TEST(Level2_InteractiveReadWriteRead);
        UNIT_TEST(Level2_InteractiveBeginReadWriteRead);
        UNIT_TEST(Level2_InteractiveReadWriteReadCommit);
        UNIT_TEST(Level2_InteractiveBeginReadWriteReadCommit);
        UNIT_TEST(Level2_InteractiveWriteOnly);
        UNIT_TEST(Level2_CompilationFailure);
        UNIT_TEST(Level2_NoFallback);
        UNIT_TEST(Level2_ActiveRequestRead);
        UNIT_TEST(Level2_ActiveRequestWrite);

        UNIT_TEST(Level3_NotInteractiveReadOnly);
        UNIT_TEST(Level3_NotInteractiveWriteOnly);
        UNIT_TEST(Level3_InteractiveReadOnly);
        UNIT_TEST(Level3_InteractiveReadWrite);
        UNIT_TEST(Level3_CompilationFailure);
        UNIT_TEST(Level3_NoFallback);
        UNIT_TEST(Level3_ActiveRequestRead);
        UNIT_TEST(Level3_ActiveRequestWrite);
    UNIT_TEST_SUITE_END();

    void Level0_NotInteractiveReadOnly();
    void Level0_NotInteractiveReadOnlySqlIn();
    void Level0_NotInteractiveWriteOnly();
    void Level0_NotInteractiveReadWrite();
    void Level0_InteractiveReadOnly();
    void Level0_CompilationFailure();
    void Level0_Fallback();

    void Level1_NotInteractiveReadOnly();
    void Level1_NotInteractiveReadOnlySqlIn();
    void Level1_NotInteractiveWriteOnly();
    void Level1_NotInteractiveReadWrite();
    void Level1_InteractiveReadOnly();
    void Level1_CompilationFailure();
    void Level1_Fallback();

    void Level2_NotInteractiveReadOnly();
    void Level2_NotInteractiveWriteOnly();
    void Level2_InteractiveReadOnly();
    void Level2_InteractiveReadWrite();
    void Level2_InteractiveBeginReadWrite();
    void Level2_InteractiveReadWriteCommit();
    void Level2_InteractiveBeginReadWriteCommit();
    void Level2_InteractiveReadWriteRead();
    void Level2_InteractiveBeginReadWriteRead();
    void Level2_InteractiveReadWriteReadCommit();
    void Level2_InteractiveBeginReadWriteReadCommit();
    void Level2_InteractiveWriteOnly();
    void Level2_CompilationFailure();
    void Level2_NoFallback();
    void Level2_ActiveRequestRead();
    void Level2_ActiveRequestWrite();

    void Level3_NotInteractiveReadOnly();
    void Level3_NotInteractiveWriteOnly();
    void Level3_InteractiveReadOnly();
    void Level3_InteractiveReadWrite();
    void Level3_CompilationFailure();
    void Level3_NoFallback();
    void Level3_ActiveRequestRead();
    void Level3_ActiveRequestWrite();

private:
    std::unique_ptr<TKikimrRunner> Kikimr;
    ::NMonitoring::TDynamicCounterPtr Counters;
    std::unique_ptr<TKqpCounters> KqpCounters;
};
UNIT_TEST_SUITE_REGISTRATION(KqpForceNewEngine);

/////////// LEVEL 0 ////////////////////////////////////////////////////////////////////////////////////////////////////
void KqpForceNewEngine::Level0_NotInteractiveReadOnly() {
    TestNotInteractiveReadOnlyTx(0);
}

void KqpForceNewEngine::Level0_NotInteractiveReadOnlySqlIn() {
    TestNotInteractiveReadOnlyTx(0, /* withSqlIn */ true);
}

void KqpForceNewEngine::Level0_NotInteractiveWriteOnly() {
    TestNotInteractiveWriteOnlyTx(0);
}

void KqpForceNewEngine::Level0_NotInteractiveReadWrite() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(Sprintf(R"(
                UPDATE `/Root/TwoShard` SET Value2 = %d WHERE Key = 1
            )", i), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 0);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level0_InteractiveReadOnly() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 1
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx = *result.GetTransaction();

            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 2
            )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[2u];["Two"];[0]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 0);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level0_CompilationFailure() {
    TestNotInteractiveReadOnlyTxFailedNECompilation(0);
}

void KqpForceNewEngine::Level0_Fallback() {
    TestNotInteractiveReadOnlyTxFallback(0);
}

/////////// LEVEL 1 ////////////////////////////////////////////////////////////////////////////////////////////////////
void KqpForceNewEngine::Level1_NotInteractiveReadOnly() {
    TestNotInteractiveReadOnlyTx(1);
}

void KqpForceNewEngine::Level1_NotInteractiveReadOnlySqlIn() {
    TestNotInteractiveReadOnlyTx(1, /* withSqlIn */ true);
}

void KqpForceNewEngine::Level1_NotInteractiveWriteOnly() {
    TestNotInteractiveWriteOnlyTx(1);
}

void KqpForceNewEngine::Level1_NotInteractiveReadWrite() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(Sprintf(R"(
                UPDATE `/Root/TwoShard` SET Value2 = %d WHERE Key = 1
            )", i), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 1);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level1_InteractiveReadOnly() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 1
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx = *result.GetTransaction();

            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 2
            )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[2u];["Two"];[0]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 1);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level1_CompilationFailure() {
    TestNotInteractiveReadOnlyTxFailedNECompilation(1);
}

void KqpForceNewEngine::Level1_Fallback() {
    TestNotInteractiveReadOnlyTxFallback(1);
}

/////////// LEVEL 2 ////////////////////////////////////////////////////////////////////////////////////////////////////
void KqpForceNewEngine::Level2_NotInteractiveReadOnly() {
    TestNotInteractiveReadOnlyTx(2);
}

void KqpForceNewEngine::Level2_NotInteractiveWriteOnly() {
    TestNotInteractiveWriteOnlyTx(2);
}

void KqpForceNewEngine::Level2_InteractiveReadOnly() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 1
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx = *result.GetTransaction();

            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 2
            )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[2u];["Two"];[0]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(50, 2);
        test(20);
        bool diff = KqpCounters->NewEngineForcedQueryCount->Val() * KqpCounters->NewEngineCompatibleQueryCount->Val() != 0;
        UNIT_ASSERT_C(diff, "forced: " << KqpCounters->NewEngineForcedQueryCount->Val()
            << ", compatible: " << KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 2);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(4, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(0, 2);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level2_InteractiveReadWrite() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ false, /* appendRead */ false, /* addCommitTx */ false);
}

void KqpForceNewEngine::Level2_InteractiveBeginReadWrite() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ true, /* appendRead */ false, /* addCommitTx */ false);
}

void KqpForceNewEngine::Level2_InteractiveReadWriteCommit() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ false, /* appendRead */ false, /* addCommitTx */ true);
}

void KqpForceNewEngine::Level2_InteractiveBeginReadWriteCommit() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ true, /* appendRead */ false, /* addCommitTx */ true);
}

void KqpForceNewEngine::Level2_InteractiveReadWriteRead() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ false, /* appendRead */ true, /* addCommitTx */ false);
}

void KqpForceNewEngine::Level2_InteractiveBeginReadWriteRead() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ true, /* appendRead */ true, /* addCommitTx */ false);
}

void KqpForceNewEngine::Level2_InteractiveReadWriteReadCommit() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ false, /* appendRead */ true, /* addCommitTx */ true);
}

void KqpForceNewEngine::Level2_InteractiveBeginReadWriteReadCommit() {
    TestInteractiveReadWriteEx(/* level */ 2, /* addBeginTx */ true, /* appendRead */ true, /* addCommitTx */ true);
}

void KqpForceNewEngine::Level2_InteractiveWriteOnly() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/TwoShard` (Key, Value1) VALUES (1, "OneOne")
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/KeyValue` (Key, Value) VALUES (1, "OneOne")
            )", TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 2);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level2_CompilationFailure() {
    TestNotInteractiveReadOnlyTxFailedNECompilation(2);
}

void KqpForceNewEngine::Level2_NoFallback() {
    TestNotInteractiveReadOnlyTxFallback(2);
}

void KqpForceNewEngine::Level2_ActiveRequestRead() {
    auto session = Session();

    // start request with OldEngine

    auto result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard` LIMIT 1
    )", TTxControl::BeginTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());

    ForceNewEngine(100, 2);

    // can switch to NewEngine (RO-query, no deferred effects)

    result = session.ExecuteDataQuery(R"(
        SELECT * FROM `/Root/TwoShard` LIMIT 1
    )", TTxControl::Tx(*result.GetTransaction())).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());

    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(2, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
}

void KqpForceNewEngine::Level2_ActiveRequestWrite() {
    auto session = Session();

    // start query with OldEngine

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TwoShard` (Key, Value1) VALUES (1, "OneOne")
    )", TTxControl::BeginTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());

    ForceNewEngine(100, 2);

    // have deferred effects, so dont force new engine on active transactions

    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction())).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineCompatibleQueryCount->Val());


    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineCompatibleQueryCount->Val());
}

/////////// LEVEL 3 ////////////////////////////////////////////////////////////////////////////////////////////////////
void KqpForceNewEngine::Level3_NotInteractiveReadOnly() {
    TestNotInteractiveReadOnlyTx(3);
}

void KqpForceNewEngine::Level3_NotInteractiveWriteOnly() {
    TestNotInteractiveWriteOnlyTx(3);
}

void KqpForceNewEngine::Level3_InteractiveReadOnly() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 1
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u];["One"];[-1]]])", FormatResultSetYson(result.GetResultSet(0)));

            auto tx = *result.GetTransaction();

            result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key = 2
            )", TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[2u];["Two"];[0]]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(50, 3);
        test(20);
        bool diff = KqpCounters->NewEngineForcedQueryCount->Val() * KqpCounters->NewEngineCompatibleQueryCount->Val() != 0;
        UNIT_ASSERT_C(diff, "forced: " << KqpCounters->NewEngineForcedQueryCount->Val()
            << ", compatible: " << KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 3);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(4, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(0, 3);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level3_InteractiveReadWrite() {
    auto session = Session();

    auto test = [&](ui32 count) {
        KqpCounters->NewEngineForcedQueryCount->Set(0);
        KqpCounters->NewEngineCompatibleQueryCount->Set(0);

        for (ui32 i = 0; i < count; ++i) {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/KeyValue` LIMIT 1
            )", TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = session.ExecuteDataQuery(Sprintf(R"(
                UPDATE `/Root/TwoShard` SET Value2 = %d WHERE Key = 1
            )", i), TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    };

    {
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(50, 3);
        test(20);
        bool diff = KqpCounters->NewEngineForcedQueryCount->Val() * KqpCounters->NewEngineCompatibleQueryCount->Val() != 0;
        UNIT_ASSERT_C(diff, "forced: " << KqpCounters->NewEngineForcedQueryCount->Val()
            << ", compatible: " << KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(100, 3);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(4, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }

    {
        ForceNewEngine(0, 3);
        test(2);
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
    }
}

void KqpForceNewEngine::Level3_CompilationFailure() {
    TestNotInteractiveReadOnlyTxFailedNECompilation(3);
}

void KqpForceNewEngine::Level3_NoFallback() {
    TestNotInteractiveReadOnlyTxFallback(3);
}

void KqpForceNewEngine::Level3_ActiveRequestRead() {
    auto session = Session();

    ForceNewEngine(1, 2);

    // start request with forced OldEngine
    TMaybe<TTransaction> tx;
    while (!tx) {
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard` LIMIT 1
        )", TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());

        if (KqpCounters->NewEngineForcedQueryCount->Val() == 1) {
            // reroll :-)

            tx->Rollback().GetValueSync();
            session = Session();

            KqpCounters->NewEngineForcedQueryCount->Set(0);
            KqpCounters->NewEngineCompatibleQueryCount->Set(0);
            continue;
        }

        UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineCompatibleQueryCount->Val());

        tx = result.GetTransaction();
    }

    ForceNewEngine(100, 3);

    // dont change to NewEngine

    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TwoShard` (Key, Value1) VALUES (1, "OneOne")
    )", TTxControl::Tx(*tx)).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineCompatibleQueryCount->Val());

    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(1, KqpCounters->NewEngineCompatibleQueryCount->Val());
}

void KqpForceNewEngine::Level3_ActiveRequestWrite() {
    auto session = Session();

    ForceNewEngine(100, 2);

    // start request with forced OldEngine
    auto result = session.ExecuteDataQuery(R"(
        REPLACE INTO `/Root/TwoShard` (Key, Value1) VALUES (1, "OneOne")
    )", TTxControl::BeginTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());

    ForceNewEngine(100, 3);

    // dont switch to NewEngine (have deferred effects)

    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction())).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = session.ExecuteDataQuery(R"(
        SELECT 42
    )", TTxControl::Tx(*result.GetTransaction()).CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->ForceNewEngineCompileErrors->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineForcedQueryCount->Val());
    UNIT_ASSERT_VALUES_EQUAL(0, KqpCounters->NewEngineCompatibleQueryCount->Val());
}

} // namespace NKqp
} // namespace NKikimr
