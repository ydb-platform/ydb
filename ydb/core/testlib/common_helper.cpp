#include "cs_helper.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::Tests::NCommon {

void THelper::WaitForSchemeOperation(TActorId sender, ui64 txId) {
    auto& runtime = *Server.GetRuntime();
    auto& settings = Server.GetSettings();
    auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
    request->Record.SetTxId(txId);
    auto tid = ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
    runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
    runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
}

void THelper::StartDataRequest(const TString& request, const bool expectSuccess) const {
    NYdb::NTable::TTableClient tClient(Server.GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
    auto expectation = expectSuccess;
    tClient.CreateSession().Subscribe([request, expectation](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
        auto session = f.GetValueSync().GetSession();
        session.ExecuteDataQuery(request
            , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx())
            .Subscribe([expectation](NYdb::NTable::TAsyncDataQueryResult f)
                {
                    TStringStream ss;
                    f.GetValueSync().GetIssues().PrintTo(ss, false);
                    Cerr << ss.Str() << Endl;
                    Y_VERIFY(expectation == f.GetValueSync().IsSuccess());
                });
        });
}

void THelper::StartSchemaRequest(const TString& request, const bool expectSuccess) const {
    NYdb::NTable::TTableClient tClient(Server.GetDriver(),
        NYdb::NTable::TClientSettings().UseQueryCache(false).AuthToken("root@builtin"));
    auto expectation = expectSuccess;
    tClient.CreateSession().Subscribe([request, expectation](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
        auto session = f.GetValueSync().GetSession();
        session.ExecuteSchemeQuery(request).Subscribe([expectation](NYdb::TAsyncStatus f)
            {
                TStringStream ss;
                f.GetValueSync().GetIssues().PrintTo(ss, false);
                Cerr << ss.Str() << Endl;
                Y_VERIFY(expectation == f.GetValueSync().IsSuccess());
            });
        });
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

}
