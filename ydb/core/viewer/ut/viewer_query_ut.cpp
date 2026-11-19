#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/script_executions/common/kqp_script_executions.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/viewer/viewer.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

namespace NKikimr::NViewer {
namespace {

enum class EFetchStage {
    ExecutionId,
    OperationId,
    NextChunk,
    NextResultSet,
};

void CheckFetchResultsUndelivered(EFetchStage stage, const TString& contentType) {
    const auto port = NTesting::GetFreePort();
    auto settings =
        Tests::TServerSettings(port).SetNodeCount(1).SetUseRealThreads(false).SetDomainName("Root").SetUseSectorMap(true).InitKikimrRunConfig();
    Tests::TServer server(settings);
    auto& runtime = *server.GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    const TString executionId = "viewer-undelivered-script";
    const bool streaming = contentType != "application/json";
    const bool hasOperation = stage == EFetchStage::OperationId || stage == EFetchStage::NextResultSet;
    const ui32 expectedFetches = stage == EFetchStage::NextChunk || stage == EFetchStage::NextResultSet ? 2 : 1;
    ui32 fetches = 0;
    ui32 operationRequests = 0;

    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NKqp::TEvGetScriptExecutionOperation::EventType) {
            ++operationRequests;
            Ydb::Query::ExecuteScriptMetadata metadata;
            metadata.set_execution_id(executionId);
            metadata.add_result_sets_meta();

            if (stage == EFetchStage::NextResultSet) {
                metadata.add_result_sets_meta();
            }

            NKqp::TEvGetScriptExecutionOperationResponse::TInfo info{ .Ready = true };
            info.Metadata.ConstructInPlace().PackFrom(metadata);
            runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                new NKqp::TEvGetScriptExecutionOperationResponse(Ydb::StatusIds::SUCCESS, std::move(info), {}), /* flags */ 0, ev->Cookie));
            return TTestActorRuntime::EEventAction::DROP;
        }

        if (ev->GetTypeRewrite() == NKqp::TEvFetchScriptResultsRequest::EventType) {
            ++fetches;
            UNIT_ASSERT(ev->Flags & IEventHandle::FlagTrackDelivery);
            const auto* request = ev->Get<NKqp::TEvFetchScriptResultsRequest>();
            UNIT_ASSERT_VALUES_EQUAL(request->ExecutionId, executionId);
            UNIT_ASSERT_VALUES_EQUAL(request->ResultSetIndex, stage == EFetchStage::NextResultSet ? fetches - 1 : 0);

            if (fetches < expectedFetches) {
                Ydb::ResultSet resultSet;
                auto* column = resultSet.add_columns();
                column->set_name("value");
                column->mutable_type()->set_type_id(Ydb::Type::INT32);
                resultSet.add_rows()->add_items()->set_int32_value(42);
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, new NKqp::TEvFetchScriptResultsResponse(Ydb::StatusIds::SUCCESS,
                                                                             std::move(resultSet), stage == EFetchStage::NextChunk, {})));
            } else {
                const TActorId unavailableProxy(runtime.GetNodeId(), "missing-kqp");
                runtime.Send(ev->Forward(unavailableProxy).Release());
            }

            return TTestActorRuntime::EEventAction::DROP;
        }

        return TTestActorRuntime::EEventAction::PROCESS;
    });

    TStringBuilder url;
    url << "/viewer/query?action=fetch-long-query&direct=1";

    if (hasOperation) {
        url << "&operation_id=" << NKqp::ScriptExecutionOperationFromExecutionId(executionId);
    } else {
        url << "&execution_id=" << executionId;
    }

    if (streaming) {
        url << "&schema=multipart";
    }

    auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
    NHttp::THttpIncomingRequestPtr request =
        new NHttp::THttpIncomingRequest(TStringBuilder() << "GET " << url << " HTTP/1.1\r\nAccept: " << contentType << "\r\n\r\n", endpoint, {});
    runtime.Send(new IEventHandle(MakeViewerID(/* nodeId */ 0), sender, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));

    const auto response = runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(sender, TDuration::Seconds(5));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Get()->Response->Status, "200");
    TString body(response->Get()->Response->Body);

    if (streaming) {
        bool finished = false;

        for (ui32 chunks = 0; chunks < 5 && !finished; ++chunks) {
            const auto chunk = runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk>(sender, TDuration::Seconds(5));
            UNIT_ASSERT(chunk);
            UNIT_ASSERT(chunk->Get()->Error.empty());
            UNIT_ASSERT(chunk->Get()->DataChunk);
            body += chunk->Get()->DataChunk->AsStringBuf();
            finished = chunk->Get()->DataChunk->IsEndOfData();
        }

        UNIT_ASSERT_C(finished, body);

        if (contentType == "text/event-stream") {
            UNIT_ASSERT_STRING_CONTAINS(body, "event: Error\n");
        } else {
            UNIT_ASSERT_STRING_CONTAINS(body, "\"event\":\"Error\"");
            UNIT_ASSERT_STRING_CONTAINS(body, "--boundary--\r\n");
        }
    } else {
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["error"]["message"].GetString(), "KQP proxy is unavailable");
        UNIT_ASSERT_VALUES_EQUAL(json["issues"][0]["message"].GetString(), "KQP proxy is unavailable");
    }

    UNIT_ASSERT_STRING_CONTAINS(body, "KQP proxy is unavailable");
    UNIT_ASSERT_VALUES_EQUAL(fetches, expectedFetches);
    UNIT_ASSERT_VALUES_EQUAL(operationRequests, hasOperation ? 1 : 0);
}

void CheckFetchResultsUndelivered(EFetchStage stage) {
    for (const TString& contentType : { "application/json", "text/event-stream", "multipart/x-mixed-replace" }) {
        CheckFetchResultsUndelivered(stage, contentType);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(ViewerQuery) {
    Y_UNIT_TEST(FetchResultsUndeliveredByExecutionId) {
        CheckFetchResultsUndelivered(EFetchStage::ExecutionId);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredByOperationId) {
        CheckFetchResultsUndelivered(EFetchStage::OperationId);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredNextChunk) {
        CheckFetchResultsUndelivered(EFetchStage::NextChunk);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredNextResultSet) {
        CheckFetchResultsUndelivered(EFetchStage::NextResultSet);
    }
}

}   // namespace NKikimr::NViewer
