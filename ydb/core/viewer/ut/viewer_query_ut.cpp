#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/script_executions/common/kqp_script_executions.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/viewer/viewer.h>

#include <ydb/library/actors/http/http_proxy.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>

#include <initializer_list>
#include <optional>

namespace NKikimr::NViewer {
namespace {

enum class EFetchStage {
    ExecutionId,
    MissingExecutionId,
    OperationId,
    NextChunk,
    NextResultSet,
};

void CheckFetchResultsError(EFetchStage stage, const TString& contentType) {
    const auto port = NTesting::GetFreePort();
    auto settings =
        Tests::TServerSettings(port).SetNodeCount(1).SetUseRealThreads(false).SetDomainName("Root").SetUseSectorMap(true).InitKikimrRunConfig();
    Tests::TServer server(settings);
    auto& runtime = *server.GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    const TString executionId = "viewer-undelivered-script";
    const bool streaming = contentType != "application/json";
    const bool hasOperation = stage == EFetchStage::OperationId || stage == EFetchStage::NextResultSet;
    const bool missing = stage == EFetchStage::MissingExecutionId;
    const TString expectedError = missing ? "Failed to fetch script results" : "KQP proxy is unavailable";
    const ui32 expectedFetches = stage == EFetchStage::NextChunk || stage == EFetchStage::NextResultSet ? 2 : 1;
    ui32 fetches = 0;
    ui32 operationRequests = 0;

    runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == NKqp::TEvGetScriptExecutionOperation::EventType) {
            ++operationRequests;
            UNIT_ASSERT_VALUES_EQUAL(ev->Get<NKqp::TEvGetScriptExecutionOperation>()->OperationId.ToString(),
                NKqp::ScriptExecutionOperationFromExecutionId(executionId));
            if (missing) {
                runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new NKqp::TEvGetScriptExecutionOperationResponse(Ydb::StatusIds::NOT_FOUND, NYql::TIssues{}), /* flags */ 0, ev->Cookie));
                return TTestActorRuntime::EEventAction::DROP;
            }

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

            UNIT_ASSERT(!missing);
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
        if (stage == EFetchStage::NextResultSet) {
            url << "&execution_id=" << executionId;
        }
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
        UNIT_ASSERT_VALUES_EQUAL(json["error"]["message"].GetString(), expectedError);
        UNIT_ASSERT_VALUES_EQUAL(json["issues"][0]["message"].GetString(), expectedError);
    }

    UNIT_ASSERT_STRING_CONTAINS(body, expectedError);
    UNIT_ASSERT_VALUES_EQUAL(fetches, missing ? 0 : expectedFetches);
    UNIT_ASSERT_VALUES_EQUAL(operationRequests, 1);
}

void CheckFetchResultsError(EFetchStage stage) {
    for (const TString& contentType : { "application/json", "text/event-stream", "multipart/x-mixed-replace" }) {
        CheckFetchResultsError(stage, contentType);
    }
}

class TScriptResultTestServer {
    const NTesting::TPortHolder Port = NTesting::GetFreePort();
    Tests::TServer Server{Tests::TServerSettings(Port)
                              .SetNodeCount(1)
                              .SetUseRealThreads(false)
                              .SetDomainName("Root")
                              .SetUseSectorMap(true)
                              .InitKikimrRunConfig()};
    TTestActorRuntime& Runtime = *Server.GetRuntime();
    const TActorId Sender = Runtime.AllocateEdgeActor();
    const TActorId Proxy = Runtime.AllocateEdgeActor();
    TInstant LastReply;

public:
    const TString ExecutionId = "viewer-runtime-results";
    const TString OperationId = NKqp::ScriptExecutionOperationFromExecutionId(ExecutionId);

    struct TSet {
        ui64 Rows;
        bool Finished = false;
    };

    TScriptResultTestServer() {
        Runtime.SetObserverFunc([&runtime = Runtime, proxy = Proxy](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if (ev->Recipient != proxy &&
                (type == NKqp::TEvGetScriptExecutionOperation::EventType || type == NKqp::TEvFetchScriptResultsRequest::EventType)) {
                runtime.EnableScheduleForActor(ev->Sender);
                runtime.Send(ev->Forward(proxy).Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void StartRequest(TStringBuf executionId, TStringBuf operationId = {}, TStringBuf schema = "classic", TStringBuf accept = "application/json") {
        TStringBuilder url;
        url << "/viewer/query?action=fetch-long-query&direct=1&max_requests_in_flight=1&schema=" << schema;
        if (executionId) {
            url << "&execution_id=" << executionId;
        }
        if (operationId) {
            url << "&operation_id=" << operationId;
        }
        auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
        NHttp::THttpIncomingRequestPtr request =
            new NHttp::THttpIncomingRequest(TStringBuilder() << "GET " << url << " HTTP/1.1\r\nAccept: " << accept << "\r\n\r\n", endpoint, {});
        Runtime.Send(new IEventHandle(MakeViewerID(/* nodeId */ 0), Sender, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));
    }

    void ReplyMetadata(std::initializer_list<TSet> sets, bool ready, bool afterDelay, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        const auto get = Runtime.GrabEdgeEventRethrow<NKqp::TEvGetScriptExecutionOperation>(Proxy, TDuration::Seconds(5));
        UNIT_ASSERT(get);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->OperationId.ToString(), OperationId);

        if (afterDelay) {
            UNIT_ASSERT_GE(Runtime.GetCurrentTime() - LastReply, TDuration::Seconds(1));
        }

        Ydb::Query::ExecuteScriptMetadata meta;
        meta.set_execution_id(ExecutionId);

        for (const auto& set : sets) {
            auto* result = meta.add_result_sets_meta();
            result->set_number_rows(set.Rows);
            result->set_finished(set.Finished);
        }

        NKqp::TEvGetScriptExecutionOperationResponse::TInfo info{ .Ready = ready, .RequestStatus = Ydb::StatusIds::SUCCESS };
        info.Metadata.ConstructInPlace().PackFrom(meta);
        LastReply = Runtime.GetCurrentTime();
        Runtime.Send(new IEventHandle(get->Sender, Proxy,
            new NKqp::TEvGetScriptExecutionOperationResponse(status, std::move(info), {}), /* flags */ 0, get->Cookie));
    }

    void ReplyPage(i32 index, i64 offset, std::optional<i32> value, bool moreRows, bool truncated = false) {
        const auto viewer = ExpectFetch(index, offset);
        Ydb::ResultSet result;
        result.set_truncated(truncated);
        auto* column = result.add_columns();
        column->set_name("value");
        column->mutable_type()->set_type_id(Ydb::Type::INT32);

        if (value) {
            result.add_rows()->add_items()->set_int32_value(*value);
        }

        Runtime.Send(new IEventHandle(
            viewer, Proxy, new NKqp::TEvFetchScriptResultsResponse(Ydb::StatusIds::SUCCESS, std::move(result), moreRows, {})));
    }

    void ReplyBadRequest(bool resultSetNotReady) {
        const auto viewer = ExpectFetch(/* index */ 0, /* offset */ 0);
        auto reply = std::make_unique<NKqp::TEvFetchScriptResultsResponse>(Ydb::StatusIds::BAD_REQUEST, NYql::TIssues{});
        reply->ResultSetNotReady = resultSetNotReady;
        Runtime.Send(new IEventHandle(viewer, Proxy, reply.release()));
    }

    NHttp::THttpOutgoingResponsePtr GetResponse() {
        const auto response = Runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(Sender, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        return response->Get()->Response;
    }

    NJson::TJsonValue GetJson() {
        const auto response = GetResponse();
        NJson::TJsonValue json;
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "200");
        UNIT_ASSERT(NJson::ReadJsonTree(response->Body, &json));
        return json;
    }

    NHttp::THttpOutgoingDataChunkPtr GetChunk() {
        const auto chunk = Runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk>(Sender, TDuration::Seconds(5));
        UNIT_ASSERT(chunk && chunk->Get()->DataChunk);
        return chunk->Get()->DataChunk;
    }

private:
    TActorId ExpectFetch(i32 index, i64 offset) {
        const auto fetch = Runtime.GrabEdgeEventRethrow<NKqp::TEvFetchScriptResultsRequest>(Proxy, TDuration::Seconds(5));
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Get()->ExecutionId, ExecutionId);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Get()->ResultSetIndex, index);
        UNIT_ASSERT_VALUES_EQUAL(fetch->Get()->Offset, offset);
        LastReply = Runtime.GetCurrentTime();
        return fetch->Sender;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(ViewerQuery) {
    Y_UNIT_TEST(WaitForRuntimeResultPagesAndResultSets) {
        TScriptResultTestServer script;
        script.StartRequest(script.ExecutionId);
        script.ReplyMetadata({}, /* ready */ false, /* afterDelay */ false);
        script.ReplyMetadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyBadRequest(/* resultSetNotReady */ true);
        script.ReplyMetadata({ { .Rows = 2 } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyPage(/* index */ 0, /* offset */ 0, /* value */ 10, /* moreRows */ true);
        script.ReplyPage(/* index */ 0, /* offset */ 1, std::nullopt, /* moreRows */ true);
        script.ReplyMetadata({ { .Rows = 2 } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyPage(/* index */ 0, /* offset */ 1, /* value */ 11, /* moreRows */ true);
        script.ReplyMetadata({ { .Rows = 2 } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyMetadata({ { .Rows = 2, .Finished = true } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyPage(/* index */ 0, /* offset */ 2, std::nullopt, /* moreRows */ false);
        script.ReplyMetadata({ { .Rows = 2, .Finished = true } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyMetadata({ { .Rows = 2, .Finished = true }, { .Rows = 0 } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyMetadata({ { .Rows = 2, .Finished = true }, { .Rows = 1 }, { .Rows = 2, .Finished = true } }, /* ready */ false, /* afterDelay */ true);
        script.ReplyPage(/* index */ 1, /* offset */ 0, /* value */ 20, /* moreRows */ false);
        script.ReplyPage(/* index */ 2, /* offset */ 0, /* value */ 30, /* moreRows */ true);
        script.ReplyPage(/* index */ 2, /* offset */ 1, /* value */ 31, /* moreRows */ false);
        script.ReplyMetadata({ { .Rows = 2, .Finished = true }, { .Rows = 1, .Finished = true }, { .Rows = 2, .Finished = true } },
            /* ready */ true, /* afterDelay */ true);

        const auto json = script.GetJson();
        const auto& rows = json["result"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]["value"].GetIntegerSafe(), 10);
        UNIT_ASSERT_VALUES_EQUAL(rows[1]["value"].GetIntegerSafe(), 11);
        UNIT_ASSERT_VALUES_EQUAL(rows[2]["value"].GetIntegerSafe(), 20);
        UNIT_ASSERT_VALUES_EQUAL(rows[3]["value"].GetIntegerSafe(), 30);
        UNIT_ASSERT_VALUES_EQUAL(rows[4]["value"].GetIntegerSafe(), 31);

        script.StartRequest(script.ExecutionId);
        script.ReplyMetadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ false);
        script.ReplyBadRequest(/* resultSetNotReady */ false);
        UNIT_ASSERT_VALUES_EQUAL(script.GetJson()["error"]["message"].GetStringSafe(), "Failed to fetch script results");
    }

    Y_UNIT_TEST(FetchStopsOnTerminalScriptFailure) {
        TScriptResultTestServer script;
        auto expectCancelled = [&] {
            script.ReplyMetadata({ { .Rows = 1 } }, /* ready */ true, /* afterDelay */ false, Ydb::StatusIds::CANCELLED);
            const auto reply = script.GetJson();
            UNIT_ASSERT_VALUES_EQUAL(reply["error"]["message"].GetStringSafe(), "CANCELLED");
            UNIT_ASSERT(!reply.Has("result"));
        };

        script.StartRequest(script.ExecutionId);
        expectCancelled();
        script.StartRequest({}, script.OperationId);
        expectCancelled();
    }

    Y_UNIT_TEST(ExecutionIdReadsPublishedRowsUntilScriptFailure) {
        TScriptResultTestServer script;
        for (TStringBuf accept : {"application/json", "text/event-stream", "multipart/x-mixed-replace"}) {
            const bool streaming = accept != "application/json";
            script.StartRequest(script.ExecutionId, {}, streaming ? "multipart" : "multi", accept);
            script.ReplyMetadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ false);
            script.ReplyPage(0, 0, 42, /* moreRows */ true);
            if (streaming) {
                UNIT_ASSERT_VALUES_EQUAL(script.GetResponse()->Status, "200");
                UNIT_ASSERT_STRING_CONTAINS(script.GetChunk()->AsStringBuf(), "\"rows\":[[42]]");
            }

            script.ReplyMetadata({ { .Rows = 2 } }, /* ready */ true, /* afterDelay */ true, Ydb::StatusIds::ABORTED);
            if (streaming) {
                const auto error = script.GetChunk();
                UNIT_ASSERT_STRING_CONTAINS(error->AsStringBuf(), accept == "text/event-stream" ? "event: Error\n" : "\"event\":\"Error\"");
                UNIT_ASSERT_STRING_CONTAINS(error->AsStringBuf(), "ABORTED");
                UNIT_ASSERT(script.GetChunk()->IsEndOfData());
            } else {
                const auto reply = script.GetJson();
                UNIT_ASSERT_VALUES_EQUAL(reply["error"]["message"].GetStringSafe(), "ABORTED");
                UNIT_ASSERT(!reply.Has("result"));
            }
        }
    }

    Y_UNIT_TEST(FetchByExecutionIdWithoutResultSets) {
        TScriptResultTestServer script;
        script.StartRequest(script.ExecutionId);
        script.ReplyMetadata({}, /* ready */ true, /* afterDelay */ false);
        UNIT_ASSERT_VALUES_EQUAL(script.GetJson()["error"]["message"].GetStringSafe(), "Failed to fetch script results");
    }

    Y_UNIT_TEST(FetchFinalPageWithTruncation) {
        TScriptResultTestServer script;
        for (TStringBuf accept : {"application/json", "text/event-stream", "multipart/x-mixed-replace"}) {
            const bool streaming = accept != "application/json";
            script.StartRequest(script.ExecutionId, {}, streaming ? "multipart" : "multi", accept);
            script.ReplyMetadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ false);
            script.ReplyPage(0, 0, 42, /* moreRows */ true);
            script.ReplyMetadata({ { .Rows = 1, .Finished = true } }, /* ready */ true, /* afterDelay */ true);
            script.ReplyPage(0, 1, std::nullopt, /* moreRows */ false, /* truncated */ true);

            if (streaming) {
                UNIT_ASSERT_VALUES_EQUAL(script.GetResponse()->Status, "200");
                UNIT_ASSERT_STRING_CONTAINS(script.GetChunk()->AsStringBuf(), "\"rows\":[[42]]");
                UNIT_ASSERT_STRING_CONTAINS(script.GetChunk()->AsStringBuf(), "\"truncated\":true");
                UNIT_ASSERT(script.GetChunk()->IsEndOfData());
            } else {
                const auto json = script.GetJson();
                const auto& result = json["result"][0];
                UNIT_ASSERT_VALUES_EQUAL(result["rows"].GetArraySafe().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result["rows"][0][0].GetIntegerSafe(), 42);
                UNIT_ASSERT(result["truncated"].GetBooleanSafe());
            }
        }
    }

    Y_UNIT_TEST(RejectConflictingScriptIds) {
        TScriptResultTestServer script;
        for (const TString& operationId : {script.OperationId, TString("malformed")}) {
            script.StartRequest("other-execution", operationId, "multipart", "text/event-stream");
            const auto response = script.GetResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->Status, "400");
            UNIT_ASSERT_STRING_CONTAINS(response->Body, "operation_id and execution_id must identify the same script execution");
        }
    }

    Y_UNIT_TEST(FetchResultsUndeliveredByExecutionId) {
        CheckFetchResultsError(EFetchStage::ExecutionId);
    }

    Y_UNIT_TEST(FetchResultsMissingExecutionId) {
        CheckFetchResultsError(EFetchStage::MissingExecutionId);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredByOperationId) {
        CheckFetchResultsError(EFetchStage::OperationId);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredNextChunk) {
        CheckFetchResultsError(EFetchStage::NextChunk);
    }

    Y_UNIT_TEST(FetchResultsUndeliveredNextResultSet) {
        CheckFetchResultsError(EFetchStage::NextResultSet);
    }
}

}   // namespace NKikimr::NViewer
