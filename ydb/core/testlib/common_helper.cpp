#include "common_helper.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/driver_lib/run/run.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::Tests::NCommon {

const std::vector<NKikimrServices::EServiceKikimr> TLoggerInit::KqpServices = {
    NKikimrServices::KQP_COMPUTE,
    NKikimrServices::KQP_GATEWAY,
    NKikimrServices::KQP_RESOURCE_MANAGER,
    NKikimrServices::KQP_EXECUTER
};

const std::vector<NKikimrServices::EServiceKikimr> TLoggerInit::CSServices = {
    NKikimrServices::TX_COLUMNSHARD,
    NKikimrServices::TX_COLUMNSHARD_BLOBS,
    NKikimrServices::TX_COLUMNSHARD_BLOBS_BS,
    NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER,
    NKikimrServices::TX_COLUMNSHARD_SCAN,
    NKikimrServices::TX_CONVEYOR
};

TLoggerInit::TLoggerInit(NKqp::TKikimrRunner& kikimr)
    : Runtime(kikimr.GetTestServer().GetRuntime()) {
}

void TLoggerInit::Initialize() {
    for (auto&& i : Services) {
        for (auto&& s : i.second) {
            Runtime->SetLogPriority(s, Priority);
        }
    }
}

void THelper::WaitForSchemeOperation(TActorId sender, ui64 txId) {
    auto& runtime = *Server.GetRuntime();
    auto& settings = Server.GetSettings();
    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void THelper::StartScanRequest(const TString& request, const bool expectSuccess, TVector<THashMap<TString, NYdb::TValue>>* result) const {
    NYdb::NTable::TTableClient tClient(Server.GetDriver(),
        NYdb::NTable::TClientSettings().UseQueryCache(false).AuthToken("root@builtin"));
    auto expectation = expectSuccess;
    bool resultReady = false;
    TVector<THashMap<TString, NYdb::TValue>> rows;
    std::optional<NYdb::NTable::TScanQueryPartIterator> scanIterator;
    tClient.StreamExecuteScanQuery(request).Subscribe([&scanIterator](NThreading::TFuture<NYdb::NTable::TScanQueryPartIterator> f) {
        scanIterator = f.GetValueSync();
    });
    const TInstant start = TInstant::Now();
    while (!resultReady && start + TDuration::Seconds(60) > TInstant::Now()) {
        Cerr << "START_SLEEP" << Endl;
        Server.GetRuntime()->SimulateSleep(TDuration::Seconds(1));
        Cerr << "FINISHED_SLEEP" << Endl;
        if (scanIterator && !resultReady) {
            scanIterator->ReadNext().Subscribe([&](NThreading::TFuture<NYdb::NTable::TScanQueryPart> streamPartFuture) {
                NYdb::NTable::TScanQueryPart streamPart = streamPartFuture.GetValueSync();
                if (!streamPart.IsSuccess()) {
                    resultReady = true;
                    UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                } else {
                    UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(), "Unexpected empty scan query response.");

                    if (streamPart.HasQueryStats()) {
                        auto plan = streamPart.GetQueryStats().GetPlan();
                        NJson::TJsonValue jsonValue;
                        if (plan) {
                            UNIT_ASSERT(NJson::ReadJsonFastTree(*plan, &jsonValue));
                            Cerr << jsonValue << Endl;
                        }
                    }

                    if (streamPart.HasResultSet()) {
                        auto resultSet = streamPart.ExtractResultSet();
                        NYdb::TResultSetParser rsParser(resultSet);
                        while (rsParser.TryNextRow()) {
                            THashMap<TString, NYdb::TValue> row;
                            for (size_t ci = 0; ci < resultSet.ColumnsCount(); ++ci) {
                                row.emplace(resultSet.GetColumnsMeta()[ci].Name, rsParser.GetValue(ci));
                                Cerr << resultSet.GetColumnsMeta()[ci].Name << "/" << rsParser.GetValue(ci).GetProto().DebugString() << Endl;
                            }
                            rows.emplace_back(std::move(row));
                        }
                    }
                }
            });
        }
    }
    Cerr << "REQUEST=" << request << ";EXPECTATION=" << expectation << Endl;
    UNIT_ASSERT(resultReady);
    if (result) {
        *result = rows;
    }
}

void THelper::StartDataRequest(const TString& request, const bool expectSuccess, TString* result) const {
    NYdb::NTable::TTableClient tClient(Server.GetDriver(),
        NYdb::NTable::TClientSettings().UseQueryCache(false).AuthToken("root@builtin"));
    auto expectation = expectSuccess;
    bool resultReady = false;
    bool* rrPtr = &resultReady;
    tClient.CreateSession().Subscribe([this, result, rrPtr, request, expectation](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
        auto session = f.GetValueSync().GetSession();
        session.ExecuteDataQuery(request
            , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx())
            .Subscribe([this, result, rrPtr, expectation, request](NYdb::NTable::TAsyncDataQueryResult f)
                {
                    TStringStream ss;
                    f.GetValueSync().GetIssues().PrintTo(ss, false);
                    Cerr << "REQUEST=" << request << ";RESULT=" << ss.Str() << ";EXPECTATION=" << expectation << Endl;
                    UNIT_ASSERT(expectation == f.GetValueSync().IsSuccess());
                    *rrPtr = true;
                    if (result && expectation) {
                        TStringStream ss;
                        NYson::TYsonWriter writer(&ss, NYson::EYsonFormat::Text);
                        for (auto&& i : f.GetValueSync().GetResultSets()) {
                            PrintResultSet(i, writer);
                        }
                        *result = ss.Str();
                    }
                });
        });
    const TInstant start = TInstant::Now();
    while (!resultReady && start + TDuration::Seconds(60) > TInstant::Now()) {
        Server.GetRuntime()->SimulateSleep(TDuration::Seconds(1));
    }
    Cerr << "REQUEST=" << request << ";EXPECTATION=" << expectation << Endl;
    UNIT_ASSERT(resultReady);
}

void THelper::StartSchemaRequestTableServiceImpl(const TString& request, const bool expectation, const bool waiting) const {
    NYdb::NTable::TTableClient tClient(Server.GetDriver(),
        NYdb::NTable::TClientSettings().UseQueryCache(false).AuthToken("root@builtin"));

    std::shared_ptr<bool> rrPtr = std::make_shared<bool>(false);
    tClient.CreateSession().Subscribe([rrPtr, request, expectation](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
        auto session = f.GetValueSync().GetSession();
        session.ExecuteSchemeQuery(request).Subscribe([rrPtr, expectation, request](NYdb::TAsyncStatus f)
            {
                TStringStream ss;
                f.GetValueSync().GetIssues().PrintTo(ss, false);
                Cerr << "REQUEST=" << request << ";RESULT=" << ss.Str() << ";EXPECTATION=" << expectation << Endl;
                UNIT_ASSERT(expectation == f.GetValueSync().IsSuccess());
                *rrPtr = true;
            });
        });
    Cerr << "REQUEST=" << request << ";EXPECTATION=" << expectation << ";WAITING=" << waiting << Endl;
    if (waiting) {
        const TInstant start = TInstant::Now();
        while (!*rrPtr && start + TDuration::Seconds(20) > TInstant::Now()) {
            Server.GetRuntime()->SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(*rrPtr);
        Cerr << "FINISHED_REQUEST=" << request << ";EXPECTATION=" << expectation << ";WAITING=" << waiting << Endl;
    }
}

void THelper::StartSchemaRequestQueryServiceImpl(const TString& request, const bool expectation, const bool waiting) const {
    NYdb::NQuery::TQueryClient qClient(Server.GetDriver(),
        NYdb::NQuery::TClientSettings().AuthToken("root@builtin"));

    std::shared_ptr<bool> rrPtr = std::make_shared<bool>(false);
    auto future = qClient.ExecuteQuery(request, NYdb::NQuery::TTxControl::NoTx());
    future.Subscribe([rrPtr, expectation, request](NYdb::NQuery::TAsyncExecuteQueryResult f)
        {
            TStringStream ss;
            f.GetValueSync().GetIssues().PrintTo(ss, false);
            Cerr << "REQUEST=" << request << ";RESULT=" << ss.Str() << ";EXPECTATION=" << expectation << Endl;
            *rrPtr = true;
        });
    Cerr << "REQUEST=" << request << ";EXPECTATION=" << expectation << ";WAITING=" << waiting << Endl;
    if (waiting) {
        const TInstant start = TInstant::Now();
        while (!*rrPtr && start + TDuration::Seconds(20) > TInstant::Now()) {
            Server.GetRuntime()->SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(*rrPtr);
        UNIT_ASSERT_C(expectation == future.GetValueSync().IsSuccess(), future.GetValueSync().GetIssues().ToString());
        Cerr << "FINISHED_REQUEST=" << request << ";EXPECTATION=" << expectation << ";WAITING=" << waiting << Endl;
    }
}

void THelper::StartSchemaRequest(const TString& request, const bool expectSuccess, const bool waiting) const {
    if (UseQueryService) {
        StartSchemaRequestQueryServiceImpl(request, expectSuccess, waiting);
    } else {
        StartSchemaRequestTableServiceImpl(request, expectSuccess, waiting);
    }
}

void THelper::DropTable(const TString& tablePath) {
    auto* runtime = Server.GetRuntime();
    Ydb::Table::DropTableRequest request;
    request.set_path(tablePath);
    size_t responses = 0;
    using TEvDropTableRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::DropTableRequest,
        Ydb::Table::DropTableResponse>;
    auto future = NRpcService::DoLocalRpc<TEvDropTableRequest>(std::move(request), "", "", runtime->GetActorSystem(0));
    future.Subscribe([&](const NThreading::TFuture<Ydb::Table::DropTableResponse> f) mutable {
        ++responses;
        UNIT_ASSERT_VALUES_EQUAL(f.GetValueSync().operation().status(), Ydb::StatusIds::SUCCESS);
        });

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return responses >= 1;
    };

    runtime->DispatchEvents(options);
}

void THelper::PrintResultSet(const NYdb::TResultSet& resultSet, NYson::TYsonWriter& writer) const {
    auto columns = resultSet.GetColumnsMeta();

    NYdb::TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        writer.OnListItem();
        writer.OnBeginList();
        for (ui32 i = 0; i < columns.size(); ++i) {
            writer.OnListItem();
            FormatValueYson(parser.GetValue(i), writer);
        }
        writer.OnEndList();
    }
}

}
