#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_connector.h>
#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/encode/json/json.h>

#include <util/generic/ptr.h>
#include <util/system/valgrind.h>

struct TStatCounters {
    i64 EndpointCount = 0;
    i64 EndpointActive = 0;
    i64 PessimizationRatio = 0;

    ui64 DiscoveryDuePessimization = 0;
    ui64 DiscoveryDueExpiration = 0;
    ui64 DiscoveryDueTransportError = 0;

    ui64 RequestFailDueNoEndpoint = 0;
    ui64 RequestFailDueQueueOverflow = 0;
    ui64 RequestFailDueTransportError = 0;

    i64 ActiveSessionsRatio = 0;
    i64 ReadySessionsRatio = 0;
    ui64 FakeSessions = 0;
    ui64 CacheMiss = 0;
    ui64 RetryOperationDueAborted = 0;
};

class TMetricEncoder: public NMonitoring::IMetricEncoder {
public:

    enum class ECounterType: size_t {

        ENDPOINTCOUNT,
        ENDPOINTACTIVE,
        ENDPOINTPESSIMIZATIONRATIO,

        ACTIVESESSIONSRATIO,
        READYSESSIONSRATIO,
        FAKESESSIONS,
        CACHEMISS,
        RETRYOPERATIONDUEABORTED,

        DISCOVERYDUEPESSIMIZATION,
        DISCOVERYDUEEXPIRATION,
        DISCOVERYFAILDUETRANSPORTERROR,

        REQUESTFAILDUEQUEUEOVERFLOW,
        REQUESTFAILDUENOENDPOINT,
        REQUESTFAILDUETRANSPORTERROR,
        REQUESTLATENCY,

        UNKNOWN
    };

    void Close() override { }

    void OnStreamBegin() override { }
    void OnStreamEnd() override { }

    void OnCommonTime(TInstant) override { }

    void OnMetricBegin(NMonitoring::EMetricType) override { }
    void OnMetricEnd() override { }

    void OnLabelsBegin() override { }
    void OnLabelsEnd() override { }

    void OnLabel(const TStringBuf name, const TStringBuf value) override {
        if (name != "sensor") {
            return;
        }

        if (value == "Discovery/TooManyBadEndpoints") {
            State = ECounterType::DISCOVERYDUEPESSIMIZATION;
        } else if (value == "Discovery/Regular") {
            State = ECounterType::DISCOVERYDUEEXPIRATION;
        } else if (value == "Request/FailedDiscoveryQueueOverflow") {
            State = ECounterType::REQUESTFAILDUEQUEUEOVERFLOW;
        } else if (value == "Request/FailedNoEndpoint") {
            State = ECounterType::REQUESTFAILDUENOENDPOINT;
        } else if (value == "Request/FailedTransportError") {
            State = ECounterType::REQUESTFAILDUETRANSPORTERROR;
        } else if (value == "Discovery/FailedTransportError") {
            State = ECounterType::DISCOVERYFAILDUETRANSPORTERROR;
        } else if (value == "Endpoints/Total") {
            State = ECounterType::ENDPOINTCOUNT;
        } else if (value == "Endpoints/BadRatio") {
            State = ECounterType::ENDPOINTPESSIMIZATIONRATIO;
        } else if (value == "Endpoints/Good") {
            State = ECounterType::ENDPOINTACTIVE;
        } else if (value == "Sessions/InUse") {
            State = ECounterType::ACTIVESESSIONSRATIO;
        } else if (value == "ready sessions ratio") {
            State = ECounterType::READYSESSIONSRATIO;
        } else if (value == "Sessions/SessionsLimitExceeded") {
            State = ECounterType::FAKESESSIONS;
        } else if (value == "Request/ClientQueryCacheMiss") {
            State = ECounterType::CACHEMISS;
        } else if (value == "RetryOperation/Aborted") {
            State = ECounterType::RETRYOPERATIONDUEABORTED;
        } else if (value == "request latency") {
            State = ECounterType::REQUESTLATENCY;
        } else {
            State = ECounterType::UNKNOWN;
        }
    }

    void OnDouble(TInstant, double) override { }

    void OnInt64(TInstant, i64 value) override {
        switch (State) {
            case ECounterType::ENDPOINTCOUNT: Counters.EndpointCount = value; break;
            case ECounterType::ENDPOINTACTIVE: Counters.EndpointActive = value; break;
            case ECounterType::ENDPOINTPESSIMIZATIONRATIO: Counters.PessimizationRatio = value; break;

            case ECounterType::ACTIVESESSIONSRATIO: Counters.ActiveSessionsRatio = value; break;
            case ECounterType::READYSESSIONSRATIO: Counters.ReadySessionsRatio = value; break;
            default: return;
        }
    }

    void OnUint64(TInstant, ui64 value) override {
        switch (State) {
            case ECounterType::DISCOVERYDUEPESSIMIZATION: Counters.DiscoveryDuePessimization = value; break;
            case ECounterType::DISCOVERYDUEEXPIRATION: Counters.DiscoveryDueExpiration = value; break;
            case ECounterType::DISCOVERYFAILDUETRANSPORTERROR: Counters.DiscoveryDueTransportError = value; break;

            case ECounterType::REQUESTFAILDUENOENDPOINT: Counters.RequestFailDueNoEndpoint = value; break;
            case ECounterType::REQUESTFAILDUEQUEUEOVERFLOW: Counters.RequestFailDueQueueOverflow = value; break;
            case ECounterType::REQUESTFAILDUETRANSPORTERROR: Counters.RequestFailDueTransportError = value; break;

            case ECounterType::FAKESESSIONS: Counters.FakeSessions = value; break;
            case ECounterType::CACHEMISS: Counters.CacheMiss = value; break;
            case ECounterType::RETRYOPERATIONDUEABORTED: Counters.RetryOperationDueAborted = value; break;
            default: return;
        }
    }

    void OnHistogram(TInstant, NMonitoring::IHistogramSnapshotPtr) override { }

    void OnLogHistogram(TInstant, NMonitoring::TLogHistogramSnapshotPtr) override { }

    void OnSummaryDouble(TInstant, NMonitoring::ISummaryDoubleSnapshotPtr) override { }

    ECounterType State = ECounterType::UNKNOWN;
    TStatCounters Counters;
};

class TCountersExtractor;

class TCountersExtractExtension: public NYdb::IExtension {
public:

    class TParams {
    public:

        TParams& SetExtractor(TCountersExtractor* extractor) {
            Extractor = extractor;
            return *this;
        }

        NMonitoring::TLabels GetApiParams() const {
            return {};
        }

        TCountersExtractor* Extractor;
    };

    using IApi = NYdb::NSdkStats::IStatApi;

    TCountersExtractExtension(const TParams& params, IApi* api);

    TStatCounters Pull() {
        TMetricEncoder extractor;
        Api_->Accept(&extractor);
        return extractor.Counters;
    }

private:
    std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry_;
    IApi* Api_ = nullptr;
};

class TCountersExtractor {
public:
    TStatCounters Extract() {
        return Extension_->Pull();
    }

    void Register(TCountersExtractExtension* extension) {
        Extension_ = extension;
    }

private:
    TCountersExtractExtension* Extension_ = nullptr;
};

TCountersExtractExtension::TCountersExtractExtension(const TParams& params, IApi* api)
    : MetricRegistry_(new NMonitoring::TMetricRegistry())
    , Api_(api)
{
    api->SetMetricRegistry(MetricRegistry_.get());
    params.Extractor->Register(this);
}

using namespace NYdb::NTable;

static const TDuration OPERATION_TIMEOUT = TDuration::Seconds(NValgrind::PlainOrUnderValgrind(5, 100));

static NYdb::TStatus SimpleSelect(TSession session, const TString& query) {
    auto txControl = NYdb::NTable::TTxControl::BeginTx().CommitTx();
    auto settings = TExecDataQuerySettings().KeepInQueryCache(true).OperationTimeout(OPERATION_TIMEOUT);
    auto result = session.ExecuteDataQuery(query, txControl, settings).GetValueSync();
    return result;
}

Y_UNIT_TEST_SUITE(ClientStatsCollector) {
    Y_UNIT_TEST(PrepareQuery) {
        NYdb::TKikimrWithGrpcAndRootSchema server;
        auto endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint));
        TCountersExtractor extractor;
        driver.AddExtension<TCountersExtractExtension>(TCountersExtractExtension::TParams().SetExtractor(&extractor));
        auto clSettings = NYdb::NTable::TClientSettings().UseQueryCache(true);
        NYdb::NTable::TTableClient client(driver, clSettings);

        auto createSessionResult = client.GetSession(TCreateSessionSettings().ClientTimeout(OPERATION_TIMEOUT)).GetValueSync();
        UNIT_ASSERT(createSessionResult.IsSuccess());

        auto session = createSessionResult.GetSession();

        {
            auto prepareResult = session.PrepareDataQuery("SELECT 1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(prepareResult.IsQueryFromCache(), false);
 
            TStatCounters counters = extractor.Extract();
            UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);

            UNIT_ASSERT(prepareResult.IsSuccess());

            {
                auto executeResult = prepareResult.GetQuery().Execute(
                    TTxControl::BeginTx(TTxSettings::SerializableRW())).GetValueSync();

                UNIT_ASSERT(executeResult.IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(executeResult.IsQueryFromCache(), false); // <- explicit prepared query
                counters = extractor.Extract();
                UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);
            }

            auto executeResult = session.ExecuteDataQuery("SELECT 1",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();

            UNIT_ASSERT(executeResult.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(executeResult.IsQueryFromCache(), true);
            counters = extractor.Extract();
            UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);
        }

        {
            auto prepareResult = session.PrepareDataQuery("SELECT 1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(prepareResult.IsQueryFromCache(), true);
            auto counters = extractor.Extract();
            UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);
        }
    }

    Y_UNIT_TEST(CounterCacheMiss) {
        NYdb::TKikimrWithGrpcAndRootSchema server;
        auto endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint));
        TCountersExtractor extractor;
        driver.AddExtension<TCountersExtractExtension>(TCountersExtractExtension::TParams().SetExtractor(&extractor));
        auto clSettings = NYdb::NTable::TClientSettings().UseQueryCache(true);
        NYdb::NTable::TTableClient client(driver, clSettings);

        auto createSessionResult = client.GetSession(TCreateSessionSettings().ClientTimeout(OPERATION_TIMEOUT)).GetValueSync();
        UNIT_ASSERT(createSessionResult.IsSuccess());

        auto session = createSessionResult.GetSession();
        UNIT_ASSERT(SimpleSelect(session, "SELECT 1;").IsSuccess());

        TStatCounters counters = extractor.Extract();
        UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);

        UNIT_ASSERT(SimpleSelect(session, "SELECT 1;").IsSuccess());

        counters = extractor.Extract();
        UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 1);

        UNIT_ASSERT(SimpleSelect(session, "SELECT 2;").IsSuccess());

        counters = extractor.Extract();
        UNIT_ASSERT_VALUES_EQUAL(counters.CacheMiss, 2);

        driver.Stop(true);
    }

    Y_UNIT_TEST(CounterRetryOperation) {
        NYdb::TKikimrWithGrpcAndRootSchema server;
        auto endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint));
        TCountersExtractor extractor;
        driver.AddExtension<TCountersExtractExtension>(TCountersExtractExtension::TParams().SetExtractor(&extractor));
        NYdb::NTable::TTableClient client(driver);

        auto retrySettings = TRetryOperationSettings().GetSessionClientTimeout(OPERATION_TIMEOUT);

        UNIT_ASSERT(client.RetryOperationSync([](TSession session){
            auto desc = TTableBuilder()
                .AddNullableColumn("id", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("name", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("id")
                .Build();

            auto settings = NYdb::NTable::TCreateTableSettings().OperationTimeout(TDuration::Seconds(100));
            return session.CreateTable("Root/names", std::move(desc), settings).GetValueSync();
        }, retrySettings).IsSuccess());

        auto settings = TExecDataQuerySettings().OperationTimeout(OPERATION_TIMEOUT);
        auto txSettings = TBeginTxSettings().OperationTimeout(OPERATION_TIMEOUT);

        auto upsertOperation = [&client, settings, retrySettings] {
            UNIT_ASSERT(client.RetryOperationSync([settings](TSession session){
                auto query = Sprintf(R"(
                    UPSERT into `Root/names` (id, name) VALUES (1, "Alex");
                )");
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).GetValueSync();
            }, retrySettings).IsSuccess());
        };

        const ui64 retriesCount = 2;
        auto retrySelectSettings = TRetryOperationSettings().MaxRetries(retriesCount).GetSessionClientTimeout(OPERATION_TIMEOUT);

        UNIT_ASSERT(client.RetryOperationSync([&upsertOperation, settings, txSettings](TSession session){
            auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW(), txSettings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(beginResult.IsSuccess(), true);

            auto tx = beginResult.GetTransaction();
            auto query = Sprintf(R"(
                SELECT * FROM `Root/names`;
                UPSERT INTO `Root/names` (id, name) VALUES (2, "Bob");
            )");
            auto queryResult = session.ExecuteDataQuery(query, TTxControl::Tx(tx), settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(queryResult.IsSuccess(), true);

            upsertOperation();

            return tx.Commit().GetValueSync();
        }, retrySelectSettings).IsSuccess() == false);

        TStatCounters counters = extractor.Extract();
        UNIT_ASSERT_VALUES_EQUAL(counters.RetryOperationDueAborted, retriesCount);

        UNIT_ASSERT(client.RetryOperation([&upsertOperation, settings, txSettings](TSession session){
            auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW(), txSettings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(beginResult.IsSuccess(), true);

            auto tx = beginResult.GetTransaction();
            auto query = Sprintf(R"(
                SELECT * FROM `Root/names`;
                UPSERT INTO `Root/names` (id, name) VALUES (2, "Bob");
            )");
            auto queryResult = session.ExecuteDataQuery(query, TTxControl::Tx(tx), settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(queryResult.IsSuccess(), true);

            upsertOperation();

            return tx.Commit().Apply([](const auto& future) {
                return NThreading::MakeFuture<NYdb::TStatus>(future.GetValue());
            });
        }, retrySelectSettings).GetValueSync().IsSuccess() == false);

        counters = extractor.Extract();
        // cumulative counter
        UNIT_ASSERT_VALUES_EQUAL(counters.RetryOperationDueAborted, retriesCount * 2);

        driver.Stop(true);
    }

    Y_UNIT_TEST(ExternalMetricRegistryByRawPtr) {
        NMonitoring::TMetricRegistry sensorsRegistry;
        NYdb::TKikimrWithGrpcAndRootSchema server;

        auto endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint));

        NSolomonStatExtension::AddMetricRegistry(driver, &sensorsRegistry);
        {
            NYdb::NTable::TTableClient client(driver);

            auto createSessionResult = client.GetSession().GetValueSync();
            auto session = createSessionResult.GetSession();
            UNIT_ASSERT(SimpleSelect(session, "SELECT 1;").IsSuccess());

            TStringStream out;
            NMonitoring::IMetricEncoderPtr encoder = NMonitoring::EncoderJson(&out);
            sensorsRegistry.Accept(TInstant::Zero(), encoder.Get());
        }
        driver.Stop(true);
    }

    template<template<typename...> class TPointer>
    void TestExternalMetricRegistry() {
        TPointer<NMonitoring::TMetricRegistry> sensorsRegistry(new NMonitoring::TMetricRegistry());
        NYdb::TKikimrWithGrpcAndRootSchema server;

        auto endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint));

        NSolomonStatExtension::AddMetricRegistry(driver, sensorsRegistry);
        {
            NYdb::NTable::TTableClient client(driver);

            auto createSessionResult = client.GetSession().GetValueSync();
            auto session = createSessionResult.GetSession();
            UNIT_ASSERT(SimpleSelect(session, "SELECT 1;").IsSuccess());

            TStringStream out;
            NMonitoring::IMetricEncoderPtr encoder = NMonitoring::EncoderJson(&out);
            sensorsRegistry->Accept(TInstant::Zero(), encoder.Get());
        }
        driver.Stop(true);
    }

    Y_UNIT_TEST(ExternalMetricRegistryStdSharedPtr) {
        TestExternalMetricRegistry<std::shared_ptr>();
    }
}
