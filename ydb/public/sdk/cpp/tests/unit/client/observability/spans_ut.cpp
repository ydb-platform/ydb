#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTests;

namespace {

constexpr const char kTestDbNamespace[] = "/Root/testdb";

struct TSpanTestParams {
    std::string Name;
    std::string ClientType;

    std::string ExecuteOp;
    std::string ExecuteOpName;

    std::string CreateSessionOp;
    std::string CreateSessionOpName;

    std::string CommitOp;
    std::string CommitOpName;

    std::string RollbackOp;
    std::string RollbackOpName;

    std::string RetryOp;
    std::string RetryOpName;
};

} // namespace

class SpanTest : public ::testing::TestWithParam<TSpanTestParams> {
protected:
    void SetUp() override {
        Tracer = std::make_shared<TFakeTracer>();
    }

    std::shared_ptr<NYdb::NObservability::TRequestSpan> MakeRequestSpan(
        const std::string& operationName,
        const std::string& endpoint,
        NTrace::ESpanKind kind = NTrace::ESpanKind::CLIENT
    ) {
        return NYdb::NObservability::TRequestSpan::Create(
            GetParam().ClientType,
            Tracer,
            operationName,
            endpoint,
            kTestDbNamespace,
            TLog{},
            kind
        );
    }

    std::shared_ptr<TFakeTracer> Tracer;
};

TEST_P(SpanTest, SpanNameFormat) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, p.ExecuteOpName);
}

TEST_P(SpanTest, SpanKindIsClient) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CreateSessionOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Kind, NTrace::ESpanKind::CLIENT);
}

TEST_P(SpanTest, DbSystemAttribute) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.system.name"), "ydb");
}

TEST_P(SpanTest, DbNamespaceAndClientApi) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.namespace"), kTestDbNamespace);
    EXPECT_EQ(fakeSpan->GetStringAttribute("ydb.client.api"), p.ClientType);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.operation.name"), p.ExecuteOpName);
}

TEST_P(SpanTest, ServerAddressAndPort) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CommitOp, "ydb.server:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "ydb.server");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_P(SpanTest, ServerAddressCustomPort) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.RollbackOp, "myhost:9090");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "myhost");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 9090);
}

TEST_P(SpanTest, ServerAddressNoPortDefaultsTo2135) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "myhost");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "myhost");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_P(SpanTest, IPv6EndpointParsing) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "[::1]:2136");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "::1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2136);
}

TEST_P(SpanTest, IPv6EndpointNoPort) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "[fe80::1]");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("server.address"), "fe80::1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("server.port"), 2135);
}

TEST_P(SpanTest, PeerEndpointAttributes) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "discovery.ydb:2135");
    span->SetPeerEndpoint("10.0.0.1:2136");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("network.peer.address"), "10.0.0.1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("network.peer.port"), 2136);
}

TEST_P(SpanTest, SuccessStatusDoesNotSetErrorAttrs) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CommitOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.response.status_code"), ToString(EStatus::SUCCESS));
    EXPECT_FALSE(fakeSpan->HasStringAttribute("error.type"));
}

TEST_P(SpanTest, ErrorStatusSetsErrorType) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.RollbackOp, "localhost:2135");
    span->End(EStatus::UNAVAILABLE);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.response.status_code"), "UNAVAILABLE");
    EXPECT_TRUE(fakeSpan->HasStringAttribute("error.type"));
    EXPECT_FALSE(fakeSpan->GetStringAttribute("error.type").empty());
}

TEST_P(SpanTest, SpanIsEndedAfterEnd) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);

    EXPECT_FALSE(fakeSpan->IsEnded());
    span->End(EStatus::SUCCESS);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_P(SpanTest, NullTracerDoesNotCrash) {
    const auto& p = GetParam();
    EXPECT_NO_THROW({
        auto span = NYdb::NObservability::TRequestSpan::Create(
            p.ClientType,
            nullptr,
            p.ExecuteOp,
            "localhost:2135",
            kTestDbNamespace,
            TLog{}
        );
        span->SetPeerEndpoint("10.0.0.1:2136");
        span->AddEvent("retry", {{"attempt", "1"}});
        span->End(EStatus::SUCCESS);
    });
}

TEST_P(SpanTest, DestructorEndsSpan) {
    const auto& p = GetParam();
    auto fakeSpan = [&]() -> std::shared_ptr<TFakeSpan> {
        auto span = MakeRequestSpan(p.CreateSessionOp, "localhost:2135");
        return Tracer->GetLastSpan();
    }();

    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_P(SpanTest, ExplicitEndThenDestructorDoesNotDoubleEnd) {
    const auto& p = GetParam();
    auto fakeSpan = [&]() -> std::shared_ptr<TFakeSpan> {
        auto span = MakeRequestSpan(p.CommitOp, "localhost:2135");
        span->End(EStatus::SUCCESS);
        return Tracer->GetLastSpan();
    }();

    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsEnded());
}

TEST_P(SpanTest, AddEventForwarded) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->AddEvent("retry", {{"ydb.attempt", "2"}, {"error.type", "UNAVAILABLE"}});
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    auto events = fakeSpan->GetEvents();
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].Name, "retry");
    EXPECT_EQ(events[0].Attributes.at("ydb.attempt"), "2");
    EXPECT_EQ(events[0].Attributes.at("error.type"), "UNAVAILABLE");
}

TEST_P(SpanTest, EmptyPeerEndpointIgnored) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CreateSessionOp, "localhost:2135");
    span->SetPeerEndpoint("");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_FALSE(fakeSpan->HasStringAttribute("network.peer.address"));
    EXPECT_FALSE(fakeSpan->HasIntAttribute("network.peer.port"));
}

TEST_P(SpanTest, OperationNamesAreNormalized) {
    const auto& p = GetParam();
    const std::vector<std::pair<std::string, std::string>> ops = {
        {p.CreateSessionOp, p.CreateSessionOpName},
        {p.ExecuteOp,       p.ExecuteOpName},
        {p.CommitOp,        p.CommitOpName},
        {p.RollbackOp,      p.RollbackOpName},
    };

    for (const auto& [op, _] : ops) {
        auto span = MakeRequestSpan(op, "localhost:2135");
        span->End(EStatus::SUCCESS);
    }

    auto spans = Tracer->GetSpans();
    ASSERT_EQ(spans.size(), ops.size());
    for (size_t i = 0; i < ops.size(); ++i) {
        EXPECT_EQ(spans[i].Name, ops[i].second);
        EXPECT_EQ(spans[i].Kind, NTrace::ESpanKind::CLIENT);
    }
}

TEST_P(SpanTest, MultipleErrorStatuses) {
    const auto& p = GetParam();
    const std::vector<std::pair<EStatus, std::string>> errorStatuses = {
        {EStatus::BAD_REQUEST,           "ydb_error"},
        {EStatus::UNAUTHORIZED,          "ydb_error"},
        {EStatus::INTERNAL_ERROR,        "ydb_error"},
        {EStatus::UNAVAILABLE,           "ydb_error"},
        {EStatus::OVERLOADED,            "ydb_error"},
        {EStatus::TIMEOUT,               "ydb_error"},
        {EStatus::NOT_FOUND,             "ydb_error"},
        {EStatus::CLIENT_INTERNAL_ERROR, "transport_error"},
    };

    for (const auto& [status, expectedErrorType] : errorStatuses) {
        auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
        span->End(status);

        auto fakeSpan = Tracer->GetLastSpan();
        ASSERT_NE(fakeSpan, nullptr);
        EXPECT_EQ(
            fakeSpan->GetStringAttribute("db.response.status_code"),
            ToString(status)
        );
        EXPECT_EQ(
            fakeSpan->GetStringAttribute("error.type"),
            expectedErrorType
        ) << "wrong error.type category for status " << static_cast<int>(status);
    }
}

TEST_P(SpanTest, EmptyEndpointDoesNotCrash) {
    const auto& p = GetParam();
    EXPECT_NO_THROW({
        auto span = MakeRequestSpan(p.ExecuteOp, "");
        span->End(EStatus::SUCCESS);
    });
}

TEST_P(SpanTest, ActivateReturnsScope) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.RetryOp, "localhost:2135");
    auto scope = span->Activate();
    EXPECT_NE(scope, nullptr);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsActivated());

    span->End(EStatus::SUCCESS);
}

TEST_P(SpanTest, ActivateNullTracerReturnsNull) {
    const auto& p = GetParam();
    auto span = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        nullptr,
        p.RetryOp,
        "localhost:2135",
        kTestDbNamespace,
        TLog{}
    );
    auto scope = span->Activate();
    EXPECT_EQ(scope, nullptr);
}

TEST_P(SpanTest, InternalSpanKindIsPropagated) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.RetryOp, "localhost:2135", NTrace::ESpanKind::INTERNAL);
    span->End(EStatus::SUCCESS);

    ASSERT_EQ(Tracer->SpanCount(), 1u);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, p.RetryOpName);
    EXPECT_EQ(Tracer->GetLastSpanRecord().Kind, NTrace::ESpanKind::INTERNAL);
}

TEST_P(SpanTest, ErrorStatusAddsExceptionEvent) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->End(EStatus::UNAVAILABLE);

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

TEST_P(SpanTest, SuccessStatusNoExceptionEvent) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CommitOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    for (const auto& event : fakeSpan->GetEvents()) {
        EXPECT_NE(event.Name, "exception");
    }
}

TEST_P(SpanTest, RecordExceptionEmitsEvent) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->RecordException("TimeoutException", "operation timed out");
    span->End(EStatus::SUCCESS);

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
            EXPECT_EQ(event.Attributes.at("exception.message"), "operation timed out");
        }
    }
    EXPECT_TRUE(found);
}

TEST_P(SpanTest, ExplicitParentIsPropagatedToTracer) {
    const auto& p = GetParam();

    auto parent = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType, Tracer, p.RetryOp, "localhost:2135", kTestDbNamespace, TLog{}, NTrace::ESpanKind::INTERNAL);
    ASSERT_NE(parent, nullptr);
    auto parentRecord = Tracer->GetLastSpanRecord();
    auto* parentRaw = parentRecord.Span.get();

    auto child = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType, Tracer, p.ExecuteOp, "localhost:2135", kTestDbNamespace,
        TLog{}, NTrace::ESpanKind::CLIENT, parent);
    ASSERT_NE(child, nullptr);
    auto childRecord = Tracer->GetLastSpanRecord();

    EXPECT_EQ(childRecord.Parent, parentRaw)
        << "child span must receive parent pointer through ITracer::StartSpan";
}

TEST_P(SpanTest, RetryAttemptParentedToRoot) {
    const auto& p = GetParam();

    auto parent = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType, Tracer, p.RetryOp, "localhost:2135", kTestDbNamespace, TLog{}, NTrace::ESpanKind::INTERNAL);
    ASSERT_NE(parent, nullptr);
    auto parentRecord = Tracer->GetLastSpanRecord();
    auto* parentRaw = parentRecord.Span.get();

    // Imitating what retry contexts do: create attempt span with explicit parent.
    auto attempt = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType, Tracer, "ydb.Try", "localhost:2135", kTestDbNamespace,
        TLog{}, NTrace::ESpanKind::INTERNAL, parent);
    ASSERT_NE(attempt, nullptr);
    auto attemptRecord = Tracer->GetLastSpanRecord();

    EXPECT_EQ(attemptRecord.Parent, parentRaw)
        << "retry attempt span must be parented to retry root span explicitly";
}

INSTANTIATE_TEST_SUITE_P(
    Clients,
    SpanTest,
    ::testing::Values(
        TSpanTestParams{
            /*Name=*/             "Query",
            /*ClientType=*/       "Query",
            /*ExecuteOp=*/        "ExecuteQuery",
            /*ExecuteOpName=*/    "ydb.ExecuteQuery",
            /*CreateSessionOp=*/  "CreateSession",
            /*CreateSessionOpName=*/"ydb.CreateSession",
            /*CommitOp=*/         "Commit",
            /*CommitOpName=*/     "ydb.Commit",
            /*RollbackOp=*/       "Rollback",
            /*RollbackOpName=*/   "ydb.Rollback",
            /*RetryOp=*/          "ydb.RunWithRetry",
            /*RetryOpName=*/      "ydb.RunWithRetry"
        },
        TSpanTestParams{
            /*Name=*/             "Table",
            /*ClientType=*/       "Table",
            /*ExecuteOp=*/        "ydb.ExecuteDataQuery",
            /*ExecuteOpName=*/    "ydb.ExecuteDataQuery",
            /*CreateSessionOp=*/  "ydb.CreateSession",
            /*CreateSessionOpName=*/"ydb.CreateSession",
            /*CommitOp=*/         "ydb.Commit",
            /*CommitOpName=*/     "ydb.Commit",
            /*RollbackOp=*/       "ydb.Rollback",
            /*RollbackOpName=*/   "ydb.Rollback",
            /*RetryOp=*/          "ydb.RunWithRetry",
            /*RetryOpName=*/      "ydb.RunWithRetry"
        }
    ),
    [](const ::testing::TestParamInfo<TSpanTestParams>& info) {
        return info.param.Name;
    }
);
