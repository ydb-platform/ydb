#include <ydb/public/sdk/cpp/src/client/impl/observability/error_category/error_category.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>
#include <ydb/public/sdk/cpp/tests/common/fake_trace_provider.h>

#include <library/cpp/logger/log.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTests;

namespace {

constexpr const char kTestDbNamespace[] = "/Root/testdb";

struct TSpanTestParams {
    std::string ClientType;

    std::string ExecuteOp;
    std::string CreateSessionOp;
    std::string CommitOp;
    std::string RollbackOp;
    std::string RetryOp;
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
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, p.ExecuteOp);
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
    EXPECT_EQ(fakeSpan->GetStringAttribute("db.operation.name"), p.ExecuteOp);
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
    EXPECT_FALSE(fakeSpan->HasStringAttribute("db.response.status_code"));
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

TEST_P(SpanTest, PeerEndpointWithNodeIdAndLocation) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "discovery.ydb:2135");
    span->SetPeerEndpoint("10.0.0.1:2136", /*nodeId=*/42, /*location=*/"vla");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("network.peer.address"), "10.0.0.1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("network.peer.port"), 2136);
    EXPECT_EQ(fakeSpan->GetIntAttribute("ydb.node.id"), 42);
    EXPECT_EQ(fakeSpan->GetStringAttribute("ydb.node.dc"), "vla");
}

TEST_P(SpanTest, PeerEndpointMissingNodeMetadataIsOmitted) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "discovery.ydb:2135");
    span->SetPeerEndpoint("10.0.0.1:2136", /*nodeId=*/0, /*location=*/"");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("network.peer.address"), "10.0.0.1");
    EXPECT_EQ(fakeSpan->GetIntAttribute("network.peer.port"), 2136);
    EXPECT_FALSE(fakeSpan->HasIntAttribute("ydb.node.id"));
    EXPECT_FALSE(fakeSpan->HasStringAttribute("ydb.node.dc"));
}

TEST_P(SpanTest, PeerEndpointSetsOnlyKnownNodeMetadata) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "discovery.ydb:2135");
    span->SetPeerEndpoint("10.0.0.1:2136", /*nodeId=*/0, /*location=*/"sas");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_EQ(fakeSpan->GetStringAttribute("ydb.node.dc"), "sas");
    EXPECT_FALSE(fakeSpan->HasIntAttribute("ydb.node.id"));
}

TEST_P(SpanTest, OperationNamesAreNormalized) {
    const auto& p = GetParam();
    const std::vector<std::string> ops = {
        p.CreateSessionOp,
        p.ExecuteOp,
        p.CommitOp,
        p.RollbackOp,
    };

    for (const auto& op : ops) {
        auto span = MakeRequestSpan(op, "localhost:2135");
        span->End(EStatus::SUCCESS);
    }

    auto spans = Tracer->GetSpans();
    ASSERT_EQ(spans.size(), ops.size());
    for (size_t i = 0; i < ops.size(); ++i) {
        EXPECT_EQ(spans[i].Name, ops[i]);
        EXPECT_EQ(spans[i].Kind, NTrace::ESpanKind::CLIENT);
    }
}

TEST_P(SpanTest, MultipleErrorStatuses) {
    const auto& p = GetParam();
    using NYdb::NObservability::kErrorTypeYdb;
    using NYdb::NObservability::kErrorTypeTransport;
    const std::vector<std::pair<EStatus, std::string_view>> errorStatuses = {
        {EStatus::BAD_REQUEST,           kErrorTypeYdb},
        {EStatus::UNAUTHORIZED,          kErrorTypeYdb},
        {EStatus::INTERNAL_ERROR,        kErrorTypeYdb},
        {EStatus::UNAVAILABLE,           kErrorTypeYdb},
        {EStatus::OVERLOADED,            kErrorTypeYdb},
        {EStatus::TIMEOUT,               kErrorTypeYdb},
        {EStatus::NOT_FOUND,             kErrorTypeYdb},
        {EStatus::CLIENT_INTERNAL_ERROR, kErrorTypeTransport},
    };

    for (const auto& [status, expectedErrorType] : errorStatuses) {
        auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
        span->End(status);

        auto fakeSpan = Tracer->GetLastSpan();
        ASSERT_NE(fakeSpan, nullptr);
        if (expectedErrorType == kErrorTypeYdb) {
            EXPECT_EQ(
                fakeSpan->GetStringAttribute("db.response.status_code"),
                ToString(status)
            );
        } else {
            EXPECT_EQ(
                fakeSpan->GetStringAttribute("db.response.status_code"),
                ""
            ) << "transport_error statuses must not set db.response.status_code (status="
              << static_cast<int>(status) << ")";
        }
        EXPECT_EQ(
            fakeSpan->GetStringAttribute("error.type"),
            std::string(expectedErrorType)
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
    EXPECT_EQ(Tracer->GetLastSpanRecord().Name, p.RetryOp);
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
            EXPECT_EQ(event.Attributes.at("exception.type"), "ydb_error");
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

TEST_P(SpanTest, ErrorStatusSetsSpanStatusError) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->End(EStatus::UNAVAILABLE);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_TRUE(fakeSpan->IsStatusSet());
    EXPECT_EQ(fakeSpan->GetStatus(), NTrace::ESpanStatus::Error);
    EXPECT_EQ(fakeSpan->GetStatusDescription(), "UNAVAILABLE");
}

TEST_P(SpanTest, SuccessStatusDoesNotSetSpanStatus) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.CommitOp, "localhost:2135");
    span->End(EStatus::SUCCESS);

    auto fakeSpan = Tracer->GetLastSpan();
    ASSERT_NE(fakeSpan, nullptr);
    EXPECT_FALSE(fakeSpan->IsStatusSet());
    EXPECT_EQ(fakeSpan->GetStatus(), NTrace::ESpanStatus::Unset);
}

TEST_P(SpanTest, EndWithExceptionEmitsEvent) {
    const auto& p = GetParam();
    auto span = MakeRequestSpan(p.ExecuteOp, "localhost:2135");
    span->EndWithException("TimeoutException", "operation timed out");
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

TEST_P(SpanTest, FirstAttemptHasNoRetryAttributes) {
    const auto& p = GetParam();

    auto root = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        Tracer,
        "RunWithRetry",
        "localhost:2135",
        kTestDbNamespace,
        TLog{},
        NTrace::ESpanKind::INTERNAL
    );
    ASSERT_NE(root, nullptr);

    auto firstAttempt = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        Tracer,
        "Try",
        "localhost:2135",
        kTestDbNamespace,
        TLog{},
        NTrace::ESpanKind::INTERNAL,
        root
    );
    ASSERT_NE(firstAttempt, nullptr);
    firstAttempt->SetRetryAttributes(/*attempt=*/0, /*backoffMs=*/0);
    auto firstAttemptRecord = Tracer->GetLastSpanRecord();

    auto child = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        Tracer,
        p.ExecuteOp,
        "localhost:2135",
        kTestDbNamespace,
        TLog{},
        NTrace::ESpanKind::CLIENT,
        firstAttempt
    );
    ASSERT_NE(child, nullptr);
    auto childRecord = Tracer->GetLastSpanRecord();

    child->End(EStatus::SUCCESS);
    firstAttempt->End(EStatus::SUCCESS);
    root->End(EStatus::SUCCESS);

    EXPECT_FALSE(firstAttemptRecord.Span->HasIntAttribute("ydb.retry.attempt"))
        << "first attempt span must not carry ydb.retry.attempt";
    EXPECT_FALSE(firstAttemptRecord.Span->HasIntAttribute("ydb.retry.backoff_ms"))
        << "first attempt span must not carry ydb.retry.backoff_ms";

    EXPECT_EQ(childRecord.Parent, firstAttemptRecord.Span.get())
        << "child RPC span must be parented to the first Try span (not to the retry root)";
}

TEST_P(SpanTest, RetryAttemptCarriesRetryAttributes) {
    const auto& p = GetParam();

    auto root = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        Tracer,
        "RunWithRetry",
        "localhost:2135",
        kTestDbNamespace,
        TLog{},
        NTrace::ESpanKind::INTERNAL
    );
    ASSERT_NE(root, nullptr);

    auto retryAttempt = NYdb::NObservability::TRequestSpan::Create(
        p.ClientType,
        Tracer,
        "Try",
        "localhost:2135",
        kTestDbNamespace,
        TLog{},
        NTrace::ESpanKind::INTERNAL,
        root
    );
    ASSERT_NE(retryAttempt, nullptr);
    retryAttempt->SetRetryAttributes(/*attempt=*/2, /*backoffMs=*/250);
    auto retryAttemptRecord = Tracer->GetLastSpanRecord();

    retryAttempt->End(EStatus::SUCCESS);
    root->End(EStatus::SUCCESS);

    EXPECT_EQ(retryAttemptRecord.Span->GetIntAttribute("ydb.retry.attempt"), 2);
    EXPECT_EQ(retryAttemptRecord.Span->GetIntAttribute("ydb.retry.backoff_ms"), 250);
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
        p.ClientType, Tracer, "Try", "localhost:2135", kTestDbNamespace,
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
            /*ClientType=*/       "Query",
            /*ExecuteOp=*/        "ExecuteQuery",
            /*CreateSessionOp=*/  "CreateSession",
            /*CommitOp=*/         "Commit",
            /*RollbackOp=*/       "Rollback",
            /*RetryOp=*/          "RunWithRetry"
        },
        TSpanTestParams{
            /*ClientType=*/       "Table",
            /*ExecuteOp=*/        "ExecuteDataQuery",
            /*CreateSessionOp=*/  "CreateSession",
            /*CommitOp=*/         "Commit",
            /*RollbackOp=*/       "Rollback",
            /*RetryOp=*/          "RunWithRetry"
        }
    ),
    [](const ::testing::TestParamInfo<TSpanTestParams>& info) {
        return info.param.ClientType;
    }
);
