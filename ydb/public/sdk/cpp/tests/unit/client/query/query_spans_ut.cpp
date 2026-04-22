#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTests;

namespace {

constexpr const char kTestDbNamespace[] = "/Root/testdb";

NYdb::NObservability::TRequestSpan MakeRequestSpan(
    std::shared_ptr<TFakeTracer> tracer,
    const std::string& operationName,
    const std::string& endpoint
) {
    return NYdb::NObservability::TRequestSpan(
        "Query",
        std::move(tracer),
        operationName,
        endpoint,
        kTestDbNamespace
    );
}

} // namespace

class QuerySpanTest : public ::testing::Test {
protected:
    void SetUp() override {
        Tracer = std::make_shared<TFakeTracer>();
    }

    std::shared_ptr<TFakeTracer> Tracer;
};

TEST_F(QuerySpanTest, SpanNameFormat) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
    span.End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, "ydb.ExecuteQuery");
}

TEST_F(QuerySpanTest, SpanKindIsClient) {
    auto span = MakeRequestSpan(Tracer, "CreateSession", "localhost:2135");
    span.End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Kind, NTrace::ESpanKind::CLIENT);
}

TEST_F(QuerySpanTest, DbSystemAttribute) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.system.name"), "ydb");
}

TEST_F(QuerySpanTest, DbNamespaceAndClientApi) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.namespace"), kTestDbNamespace);
    EXPECT_EQ(fakeSpan->GetStringAttribute("ydb.client.api"), "Query");
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.operation.name"), "ydb.ExecuteQuery");
}

TEST_F(QuerySpanTest, ServerAddressAndPort) {
    auto span = MakeRequestSpan(Tracer, "Commit", "ydb.server:2135");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "ydb.server");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_F(QuerySpanTest, ServerAddressCustomPort) {
    auto span = MakeRequestSpan(Tracer, "Rollback", "myhost:9090");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "myhost");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 9090);
}

TEST_F(QuerySpanTest, ServerAddressNoPortDefaultsTo2135) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "myhost");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "myhost");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_F(QuerySpanTest, IPv6EndpointParsing) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "[::1]:2136");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "::1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2136);
}

TEST_F(QuerySpanTest, IPv6EndpointNoPort) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "[fe80::1]");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "fe80::1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_F(QuerySpanTest, PeerEndpointAttributes) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "discovery.ydb:2135");
    span.SetPeerEndpoint("10.0.0.1:2136");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("network.peer.address"), "10.0.0.1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("network.peer.port"), 2136);
}

TEST_F(QuerySpanTest, SuccessStatusDoesNotSetErrorAttrs) {
    auto span = MakeRequestSpan(Tracer, "Commit", "localhost:2135");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_FALSE(fakeSpan->HasStringAttribute("db.response.status_code"));
    EXPECT_FALSE(fakeSpan->HasStringAttribute("error.type"));
}

TEST_F(QuerySpanTest, ErrorStatusSetsErrorType) {
    auto span = MakeRequestSpan(Tracer, "Rollback", "localhost:2135");
    span.End(EStatus::UNAVAILABLE);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.response.status_code"), "UNAVAILABLE");
    EXPECT_TRUE(fakeSpan->HasStringAttribute("error.type"));
    EXPECT_FALSE(fakeSpan->GetStringAttribute("error.type").empty());
}

TEST_F(QuerySpanTest, SpanIsEndedAfterEnd) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);

    EXPECT_FALSE(fakeSpan->IsEnded());
    span.End(EStatus::SUCCESS);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_F(QuerySpanTest, NullTracerDoesNotCrash) {
    EXPECT_NO_THROW({
        NYdb::NObservability::TRequestSpan span(
            "Query",
            nullptr,
            "ExecuteQuery",
            "localhost:2135",
            kTestDbNamespace
        );
        span.SetPeerEndpoint("10.0.0.1:2136");
        span.AddEvent("retry", {{"attempt", "1"}});
        span.End(EStatus::SUCCESS);
    });
}

TEST_F(QuerySpanTest, DestructorEndsSpan) {
    auto fakeSpan = [&]() -> std::shared_ptr<TFakeSpan> {
        auto span = MakeRequestSpan(Tracer, "CreateSession", "localhost:2135");
        return Tracer->GetLastSpan();
    }();

    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_F(QuerySpanTest, ExplicitEndThenDestructorDoesNotDoubleEnd) {
    auto fakeSpan = [&]() -> std::shared_ptr<TFakeSpan> {
        auto span = MakeRequestSpan(Tracer, "Commit", "localhost:2135");
        span.End(EStatus::SUCCESS);
        return Tracer->GetLastSpan();
    }();

    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_F(QuerySpanTest, AddEventForwarded) {
    auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
    span.AddEvent("retry", {{"ydb.attempt", "2"}, {"error.type", "UNAVAILABLE"}});
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    auto events = fakeSpan->GetEvents();
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].Name, "retry");
    EXPECT_EQ(events[0].Attributes.at("ydb.attempt"), "2");
    EXPECT_EQ(events[0].Attributes.at("error.type"), "UNAVAILABLE");
}

TEST_F(QuerySpanTest, EmptyPeerEndpointIgnored) {
    auto span = MakeRequestSpan(Tracer, "CreateSession", "localhost:2135");
    span.SetPeerEndpoint("");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_FALSE(fakeSpan->HasStringAttribute("network.peer.address"));
    EXPECT_FALSE(fakeSpan->HasIntAttribute("network.peer.port"));
}

TEST_F(QuerySpanTest, AllFourOperationNames) {
    const std::vector<std::string> operations = {"CreateSession", "ExecuteQuery", "Commit", "Rollback"};

    for (const auto& op : operations) {
        auto span = MakeRequestSpan(Tracer, op, "localhost:2135");
        span.End(EStatus::SUCCESS);
    }

    auto spans = Tracer->GetSpans();
    ASSERT_EQ(spans.size(), 4u);
    EXPECT_EQ(spans[0].Name, "ydb.CreateSession");
    EXPECT_EQ(spans[1].Name, "ydb.ExecuteQuery");
    EXPECT_EQ(spans[2].Name, "ydb.Commit");
    EXPECT_EQ(spans[3].Name, "ydb.Rollback");

    for (const auto& record : spans) {
        EXPECT_EQ(record.Kind, NTrace::ESpanKind::CLIENT);
    }
}

TEST_F(QuerySpanTest, MultipleErrorStatuses) {
    std::vector<EStatus> errorStatuses = {
        EStatus::BAD_REQUEST,
        EStatus::UNAUTHORIZED,
        EStatus::INTERNAL_ERROR,
        EStatus::UNAVAILABLE,
        EStatus::OVERLOADED,
        EStatus::TIMEOUT,
        EStatus::NOT_FOUND,
        EStatus::CLIENT_INTERNAL_ERROR,
    };

    for (auto status : errorStatuses) {
        auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "localhost:2135");
        span.End(status);

        auto fakeSpan = Tracer->GetLastSpan();
        ASSERT_NE(fakeSpan, nullptr);
        EXPECT_TRUE(fakeSpan->HasStringAttribute("error.type"))
            << "error.type missing for status " << static_cast<int>(status);
        EXPECT_EQ(fakeSpan->GetStringAttribute("db.response.status_code"),
                  fakeSpan->GetStringAttribute("error.type"));
    }
}

TEST_F(QuerySpanTest, EmptyEndpointDoesNotCrash) {
    EXPECT_NO_THROW({
        auto span = MakeRequestSpan(Tracer, "ExecuteQuery", "");
        span.End(EStatus::SUCCESS);
    });
}

TEST_F(QuerySpanTest, ActivateReturnsScope) {
    auto span = MakeRequestSpan(Tracer, "RetryQuery", "localhost:2135");
    auto scope = span.Activate();
    EXPECT_NE(scope, nullptr);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsActivated());

    span.End(EStatus::SUCCESS);
}

TEST_F(QuerySpanTest, ActivateNullTracerReturnsNull) {
    NYdb::NObservability::TRequestSpan span(
        "Query",
        nullptr,
        "RetryQuery",
        "localhost:2135",
        kTestDbNamespace
    );
    auto scope = span.Activate();
    EXPECT_EQ(scope, nullptr);
}

TEST_F(QuerySpanTest, InternalSpanKindIsPropagated) {
    NYdb::NObservability::TRequestSpan span(
        "Query",
        Tracer,
        "ydb.RunWithRetry",
        "localhost:2135",
        kTestDbNamespace,
        TLog(),
        NTrace::ESpanKind::INTERNAL
    );
    span.End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, "ydb.RunWithRetry");
    EXPECT_EQ(Tracer->GetLastSpanRecord().Kind, NTrace::ESpanKind::INTERNAL);
}

TEST_F(QuerySpanTest, ErrorStatusAddsExceptionEvent) {
    auto span = MakeRequestSpan(Tracer, "ydb.ExecuteQuery", "localhost:2135");
    span.End(EStatus::UNAVAILABLE);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    auto events = fakeSpan->GetEvents();
    ASSERT_FALSE(events.empty());

    bool found = false;
    for (const auto& event : events) {
        if (event.Name == "exception") {
            found = true;
            EXPECT_EQ(event.Attributes.at("exception.type"), "UNAVAILABLE");
            EXPECT_EQ(event.Attributes.at("exception.message"), "UNAVAILABLE");
        }
    }
    EXPECT_TRUE(found) << "expected an 'exception' event on a failed span";
}

TEST_F(QuerySpanTest, SuccessStatusNoExceptionEvent) {
    auto span = MakeRequestSpan(Tracer, "ydb.Commit", "localhost:2135");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    for (const auto& event : fakeSpan->GetEvents()) {
        EXPECT_NE(event.Name, "exception");
    }
}

TEST_F(QuerySpanTest, RecordExceptionEmitsEvent) {
    auto span = MakeRequestSpan(Tracer, "ydb.ExecuteQuery", "localhost:2135");
    span.RecordException("TimeoutException", "query timed out");
    span.End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    auto events = fakeSpan->GetEvents();
    ASSERT_FALSE(events.empty());
    bool found = false;
    for (const auto& event : events) {
        if (event.Name == "exception"
            && event.Attributes.count("exception.type")
            && event.Attributes.at("exception.type") == "TimeoutException")
        {
            found = true;
            EXPECT_EQ(event.Attributes.at("exception.message"), "query timed out");
        }
    }
    EXPECT_TRUE(found);
}
