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
    UNIT_ASSERT_VALUES_EQUAL(operationRequests, 1);
}

void CheckFetchResultsUndelivered(EFetchStage stage) {
    for (const TString& contentType : { "application/json", "text/event-stream", "multipart/x-mixed-replace" }) {
        CheckFetchResultsUndelivered(stage, contentType);
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(ViewerQuery) {
    Y_UNIT_TEST(WaitForRuntimeResultPagesAndResultSets) {
        const auto port = NTesting::GetFreePort();
        auto settings = Tests::TServerSettings(port)
                            .SetNodeCount(1)
                            .SetUseRealThreads(false)
                            .SetDomainName("Root")
                            .SetUseSectorMap(true)
                            .InitKikimrRunConfig();
        Tests::TServer server(settings);
        auto& runtime = *server.GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();

        const auto proxy = runtime.AllocateEdgeActor();
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            const auto type = ev->GetTypeRewrite();
            if (ev->Recipient != proxy &&
                (type == NKqp::TEvGetScriptExecutionOperation::EventType || type == NKqp::TEvFetchScriptResultsRequest::EventType)) {
                runtime.EnableScheduleForActor(ev->Sender);
                runtime.Send(ev->Forward(proxy).Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const TString executionId = "viewer-runtime-results";
        auto startRequest = [&] {
            auto endpoint = std::make_shared<NHttp::THttpEndpointInfo>();
            NHttp::THttpIncomingRequestPtr request =
                new NHttp::THttpIncomingRequest(TStringBuilder() << "GET /viewer/query?action=fetch-long-query&execution_id=" << executionId
                                                                 << "&direct=1&max_requests_in_flight=1 HTTP/1.1\r\n\r\n", endpoint,
                    {});
            runtime.Send(new IEventHandle(MakeViewerID(/* nodeId */ 0), sender, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));
        };

        struct TSet {
            ui64 Rows;
            bool Finished = false;
        };

        TInstant lastReply;
        auto metadata = [&](std::initializer_list<TSet> sets, bool ready, bool afterDelay) {
            const auto get = runtime.GrabEdgeEventRethrow<NKqp::TEvGetScriptExecutionOperation>(proxy, TDuration::Seconds(5));
            UNIT_ASSERT(get);
            UNIT_ASSERT_VALUES_EQUAL(get->Get()->OperationId.ToString(), NKqp::ScriptExecutionOperationFromExecutionId(executionId));

            if (afterDelay) {
                UNIT_ASSERT_GE(runtime.GetCurrentTime() - lastReply, TDuration::Seconds(1));
            }

            Ydb::Query::ExecuteScriptMetadata meta;
            meta.set_execution_id(executionId);

            for (const auto& set : sets) {
                auto* result = meta.add_result_sets_meta();
                result->set_number_rows(set.Rows);
                result->set_finished(set.Finished);
            }

            NKqp::TEvGetScriptExecutionOperationResponse::TInfo info{ .Ready = ready };
            info.Metadata.ConstructInPlace().PackFrom(meta);
            lastReply = runtime.GetCurrentTime();
            runtime.Send(new IEventHandle(get->Sender, proxy,
                new NKqp::TEvGetScriptExecutionOperationResponse(Ydb::StatusIds::SUCCESS, std::move(info), {}), /* flags */ 0, get->Cookie));
        };

        auto expectFetch = [&](i32 index, i64 offset) {
            const auto fetch = runtime.GrabEdgeEventRethrow<NKqp::TEvFetchScriptResultsRequest>(proxy, TDuration::Seconds(5));
            UNIT_ASSERT(fetch);
            UNIT_ASSERT_VALUES_EQUAL(fetch->Get()->ResultSetIndex, index);
            UNIT_ASSERT_VALUES_EQUAL(fetch->Get()->Offset, offset);
            lastReply = runtime.GetCurrentTime();
            return fetch->Sender;
        };

        auto page = [&](i32 index, i64 offset, std::optional<i32> value, bool moreRows) {
            const auto viewer = expectFetch(index, offset);
            Ydb::ResultSet result;
            auto* column = result.add_columns();
            column->set_name("value");
            column->mutable_type()->set_type_id(Ydb::Type::INT32);

            if (value) {
                result.add_rows()->add_items()->set_int32_value(*value);
            }

            runtime.Send(new IEventHandle(
                viewer, proxy, new NKqp::TEvFetchScriptResultsResponse(Ydb::StatusIds::SUCCESS, std::move(result), moreRows, {})));
        };

        auto badRequest = [&](bool resultSetNotReady) {
            const auto viewer = expectFetch(/* index */ 0, /* offset */ 0);
            auto reply = std::make_unique<NKqp::TEvFetchScriptResultsResponse>(Ydb::StatusIds::BAD_REQUEST, NYql::TIssues{});
            reply->ResultSetNotReady = resultSetNotReady;
            runtime.Send(new IEventHandle(viewer, proxy, reply.release()));
        };

        startRequest();
        metadata({}, /* ready */ false, /* afterDelay */ false);
        metadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ true);
        badRequest(/* resultSetNotReady */ true);
        metadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ true);
        page(/* index */ 0, /* offset */ 0, /* value */ 10, /* moreRows */ true);
        page(/* index */ 0, /* offset */ 1, std::nullopt, /* moreRows */ true);
        metadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ true);
        metadata({ { .Rows = 2, .Finished = true } }, /* ready */ false, /* afterDelay */ true);
        page(/* index */ 0, /* offset */ 1, /* value */ 11, /* moreRows */ false);
        metadata({ { .Rows = 2, .Finished = true } }, /* ready */ false, /* afterDelay */ true);
        metadata({ { .Rows = 2, .Finished = true }, { .Rows = 0 } }, /* ready */ false, /* afterDelay */ true);
        metadata({ { .Rows = 2, .Finished = true }, { .Rows = 1 } }, /* ready */ false, /* afterDelay */ true);
        page(/* index */ 1, /* offset */ 0, /* value */ 20, /* moreRows */ false);
        metadata({ { .Rows = 2, .Finished = true }, { .Rows = 1, .Finished = true } }, /* ready */ true, /* afterDelay */ true);

        const auto response = runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Get()->Response->Body, &json));
        const auto& rows = json["result"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(rows[0]["value"].GetIntegerSafe(), 10);
        UNIT_ASSERT_VALUES_EQUAL(rows[1]["value"].GetIntegerSafe(), 11);
        UNIT_ASSERT_VALUES_EQUAL(rows[2]["value"].GetIntegerSafe(), 20);

        startRequest();
        metadata({ { .Rows = 1 } }, /* ready */ false, /* afterDelay */ false);
        badRequest(/* resultSetNotReady */ false);
        const auto error = runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(error->Get()->Response->Body, "Failed to fetch script results");
    }

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
